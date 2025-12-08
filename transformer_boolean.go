package dameng

import (
	"regexp"
)

// BooleanSyntaxTransformer 布尔语法转换器，将 MySQL 的 IS TRUE/IS FALSE 语法替换为达梦兼容的 = 1/= 0 语法
type BooleanSyntaxTransformer struct {
	isTrueRegex     *regexp.Regexp
	isFalseRegex    *regexp.Regexp
	isNotTrueRegex  *regexp.Regexp
	isNotFalseRegex *regexp.Regexp
}

// NewBooleanSyntaxTransformer 创建一个新的布尔语法转换器
func NewBooleanSyntaxTransformer() *BooleanSyntaxTransformer {
	return &BooleanSyntaxTransformer{
		// 所有的布尔语法都替换为 = true/= false，包括字符串内部的内容
		isTrueRegex:     regexp.MustCompile(`(?i)\bIS\s+TRUE\b`),
		isFalseRegex:    regexp.MustCompile(`(?i)\bIS\s+FALSE\b`),
		isNotTrueRegex:  regexp.MustCompile(`(?i)\bIS\s+NOT\s+TRUE\b`),
		isNotFalseRegex: regexp.MustCompile(`(?i)\bIS\s+NOT\s+FALSE\b`),
	}
}

// Transform 实现 SQLTransformer 接口，转换 SQL 语句中的布尔语法
func (t *BooleanSyntaxTransformer) Transform(sql string) string {
	// 注意替换顺序：先处理 IS NOT，再处理 IS
	// 这样可以避免 "IS NOT TRUE" 被先替换成 "= true NOT TRUE"
	sql = t.isNotTrueRegex.ReplaceAllString(sql, "= false")
	sql = t.isNotFalseRegex.ReplaceAllString(sql, "= true")
	sql = t.isTrueRegex.ReplaceAllString(sql, "= true")
	sql = t.isFalseRegex.ReplaceAllString(sql, "= false")
	return sql
}
