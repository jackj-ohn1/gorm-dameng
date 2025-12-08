package dameng

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

// SQLTransformer SQL转换器接口，用于在SQL执行前对SQL语句进行转换
type SQLTransformer interface {
	// Transform 转换SQL语句
	Transform(sql string) string
}

// ConnPoolWrapper 包装 gorm.ConnPool，在SQL执行前通过转换器进行SQL转换
type ConnPoolWrapper struct {
	gorm.ConnPool
	transformers []SQLTransformer
}

// ExecContext 执行SQL语句前进行转换
func (wrapper *ConnPoolWrapper) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	query = wrapper.transformSQL(query)
	return wrapper.ConnPool.ExecContext(ctx, query, args...)
}

// QueryContext 查询SQL语句前进行转换
func (wrapper *ConnPoolWrapper) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	query = wrapper.transformSQL(query)
	return wrapper.ConnPool.QueryContext(ctx, query, args...)
}

// QueryRowContext 查询单行SQL语句前进行转换
func (wrapper *ConnPoolWrapper) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	query = wrapper.transformSQL(query)
	return wrapper.ConnPool.QueryRowContext(ctx, query, args...)
}

// PrepareContext 准备SQL语句前进行转换
func (wrapper *ConnPoolWrapper) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	query = wrapper.transformSQL(query)
	return wrapper.ConnPool.PrepareContext(ctx, query)
}

// transformSQL 应用所有转换器
func (wrapper *ConnPoolWrapper) transformSQL(sql string) string {
	for _, transformer := range wrapper.transformers {
		sql = transformer.Transform(sql)
	}
	return sql
}

// AddTransformer 添加SQL转换器
func (wrapper *ConnPoolWrapper) AddTransformer(transformer SQLTransformer) {
	wrapper.transformers = append(wrapper.transformers, transformer)
}

// WrapConnPool 包装 ConnPool 并添加默认的SQL转换器
func WrapConnPool(pool gorm.ConnPool, transformers ...SQLTransformer) gorm.ConnPool {
	// 如果已经是包装过的，添加新的转换器
	if w, ok := pool.(*ConnPoolWrapper); ok {
		for _, t := range transformers {
			w.AddTransformer(t)
		}
		return w
	}
	
	wrapper := &ConnPoolWrapper{
		ConnPool:     pool,
		transformers: make([]SQLTransformer, 0),
	}
	
	// 添加传入的转换器
	for _, t := range transformers {
		wrapper.AddTransformer(t)
	}
	
	return wrapper
}

