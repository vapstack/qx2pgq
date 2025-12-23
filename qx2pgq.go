package qx2pgq

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/henvic/pgq"
	"github.com/vapstack/qx"
)

func Select(b pgq.SelectBuilder, q *qx.QX) (pgq.SelectBuilder, error) {
	if q == nil {
		return b, nil
	}

	if q.Offset > 0 {
		b = b.Offset(q.Offset)
	}
	if q.Limit > 0 {
		b = b.Limit(q.Limit)
	}

	if q.Expr.Op != qx.OpNOOP {
		e, err := buildExpr(q.Expr)
		if err != nil {
			return b, fmt.Errorf("error building expression: %w", err)
		}
		if e != nil {
			b = b.Where(e)
		}
	}

	for _, order := range q.Order {
		if order.Field == "" {
			continue
		}

		switch order.Type {

		case qx.OrderBasic:
			direction := "ASC"
			if order.Desc {
				direction = "DESC"
			}
			b = b.OrderBy(fmt.Sprintf("%v %v", order.Field, direction))

		case qx.OrderByArrayPos:

			clause := fmt.Sprintf("array_position(?, %v)", order.Field)
			if order.Desc {
				clause += " DESC"
			}
			b = b.OrderByClause(clause, order.Data)

		case qx.OrderByArrayCount:

			clause := fmt.Sprintf("cardinality(%v)", order.Field)
			if order.Desc {
				clause += " DESC"
			}
			b = b.OrderBy(clause)
		}
	}
	return b, nil
}

func Update(b pgq.UpdateBuilder, q *qx.QX) (pgq.UpdateBuilder, error) {
	if q == nil || !hasConditions(q.Expr) {
		return b, fmt.Errorf("UPDATE without conditions is not permitted")
	}
	if q.Expr.Op != qx.OpNOOP {
		e, err := buildExpr(q.Expr)
		if err != nil {
			return b, fmt.Errorf("error building expression: %w", err)
		}
		if e != nil {
			b = b.Where(e)
		}
	}
	return b, nil
}

func Delete(b pgq.DeleteBuilder, q *qx.QX) (pgq.DeleteBuilder, error) {
	if q == nil || !hasConditions(q.Expr) {
		return b, fmt.Errorf("DELETE without conditions is not permitted")
	}

	e, err := buildExpr(q.Expr)
	if err != nil {
		return b, fmt.Errorf("error building expression: %w", err)
	}
	if e != nil {
		b = b.Where(e)
	}

	return b, nil
}

func buildExpr(exp qx.Expr) (pgq.SQLizer, error) {
	switch exp.Op {
	case qx.OpNOOP:
		return nil, nil

	case qx.OpAND, qx.OpOR:
		return buildLogical(exp)

	case qx.OpEQ, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		return buildComparison(exp)

	case qx.OpIN:
		return buildIn(exp)

	case qx.OpHAS, qx.OpHASANY:
		return buildArrayOp(exp)

	case qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
		return buildStringOp(exp)

	default:
		return nil, fmt.Errorf("unknown op: %v", exp.Op)
	}
}

func buildLogical(exp qx.Expr) (pgq.SQLizer, error) {
	if len(exp.Operands) == 0 {
		return nil, nil
	}

	list := make([]pgq.SQLizer, 0, len(exp.Operands))
	for _, subExp := range exp.Operands {
		res, err := buildExpr(subExp)
		if err != nil {
			return nil, err
		}
		if res != nil {
			list = append(list, res)
		}
	}

	if len(list) == 0 {
		return nil, nil
	}

	var res pgq.SQLizer
	if exp.Op == qx.OpAND {
		res = pgq.And(list...)
	} else {
		res = pgq.Or(list...)
	}

	if exp.Not {
		sql, args, err := res.SQL()
		if err != nil {
			return nil, err
		}
		return pgq.Expr("NOT ("+sql+")", args...), nil
	}

	return res, nil
}

func buildComparison(exp qx.Expr) (pgq.SQLizer, error) {
	if !isComparable(exp.Value) {
		return nil, fmt.Errorf("value for field %s is not comparable (kind: %v)", exp.Field, reflect.TypeOf(exp.Value).Kind())
	}

	switch exp.Op {
	case qx.OpEQ:
		if exp.Not {
			return pgq.NotEq{exp.Field: exp.Value}, nil
		}
		return pgq.Eq{exp.Field: exp.Value}, nil

	case qx.OpGT:
		if exp.Not {
			return pgq.LtOrEq{exp.Field: exp.Value}, nil
		}
		return pgq.Gt{exp.Field: exp.Value}, nil

	case qx.OpGTE:
		if exp.Not {
			return pgq.Lt{exp.Field: exp.Value}, nil
		}
		return pgq.GtOrEq{exp.Field: exp.Value}, nil

	case qx.OpLT:
		if exp.Not {
			return pgq.GtOrEq{exp.Field: exp.Value}, nil
		}
		return pgq.Lt{exp.Field: exp.Value}, nil

	case qx.OpLTE:
		if exp.Not {
			return pgq.Gt{exp.Field: exp.Value}, nil
		}
		return pgq.LtOrEq{exp.Field: exp.Value}, nil
	}
	return nil, nil
}

func buildIn(exp qx.Expr) (pgq.SQLizer, error) {
	if !isSlice(exp.Value) {
		return nil, fmt.Errorf("value for %v must be a slice (field: %s)", exp.Op, exp.Field)
	}
	if exp.Not {
		return pgq.NotEq{exp.Field: exp.Value}, nil
	}
	return pgq.Eq{exp.Field: exp.Value}, nil
}

func buildArrayOp(exp qx.Expr) (pgq.SQLizer, error) {
	if !isSlice(exp.Value) {
		return nil, fmt.Errorf("value for %v must be a slice (field: %v)", exp.Op, exp.Field)
	}

	// HAS: field @> value (contains)
	// HASANY: field && value (overlaps)

	op := "@>"
	if exp.Op == qx.OpHASANY {
		op = "&&"
	}

	if exp.Not {
		return pgq.Expr(fmt.Sprintf("NOT (%s %s ?)", exp.Field, op), exp.Value), nil
	}
	return pgq.Expr(exp.Field+" "+op+" ?", exp.Value), nil
}

func buildStringOp(exp qx.Expr) (pgq.SQLizer, error) {
	strVal, ok := exp.Value.(string)
	if !ok {
		return nil, fmt.Errorf("value for %s must be a string (field: %s)", exp.Op, exp.Field)
	}

	escapedVal := likeEscapeReplacer.Replace(strVal)

	var pattern string
	switch exp.Op {
	case qx.OpPREFIX:
		pattern = escapedVal + "%"
	case qx.OpSUFFIX:
		pattern = "%" + escapedVal
	case qx.OpCONTAINS:
		pattern = "%" + escapedVal + "%"
	}

	if exp.Not {
		return pgq.Expr(exp.Field+" NOT LIKE ?", exp.Field, pattern), nil
	}
	return pgq.Expr(exp.Field+" LIKE ?", pattern), nil
}

func hasConditions(expr qx.Expr) bool {
	if expr.Op != qx.OpAND && expr.Op != qx.OpOR && expr.Op != qx.OpNOOP {
		return true
	}
	for _, operand := range expr.Operands {
		if hasConditions(operand) {
			return true
		}
	}
	return false
}

func isComparable(v any) bool {
	if v == nil {
		return true // converted to IS NULL by pgq
	}
	rt := reflect.ValueOf(v)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	return rt.Comparable()
}

func isSlice(v any) bool {
	if v == nil {
		return false
	}
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	return rt.Kind() == reflect.Slice
}

var likeEscapeReplacer = strings.NewReplacer(
	"\\", "\\\\",
	"%", "\\%",
	"_", "\\_",
)
