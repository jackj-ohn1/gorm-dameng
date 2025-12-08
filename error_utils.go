package dameng

import (
	"errors"

	"github.com/jackj-ohn1/gorm-dameng/dm8"
)

// IsDuplicateKeyError 判断错误是否是重复键错误（主键或唯一约束冲突）
func IsDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	var dmErr *dm8.DmError
	if errors.As(err, &dmErr) {
		// -6602: 主键冲突
		// -6603: 唯一约束冲突
		return dmErr.ErrCode == -6602 || dmErr.ErrCode == -6603
	}

	return false
}

// GetErrorInfo 获取达梦数据库错误的详细信息
func GetErrorInfo(err error) (errCode int32, errText string, ok bool) {
	if err == nil {
		return 0, "", false
	}

	var dmErr *dm8.DmError
	if errors.As(err, &dmErr) {
		return dmErr.ErrCode, dmErr.ErrText, true
	}

	return 0, "", false
}

// IsPrimaryKeyError 判断是否是主键冲突错误
func IsPrimaryKeyError(err error) bool {
	if err == nil {
		return false
	}

	var dmErr *dm8.DmError
	if errors.As(err, &dmErr) {
		return dmErr.ErrCode == -6602
	}

	return false
}

// IsUniqueIndexError 判断是否是唯一约束冲突错误
func IsUniqueIndexError(err error) bool {
	if err == nil {
		return false
	}

	var dmErr *dm8.DmError
	if errors.As(err, &dmErr) {
		return dmErr.ErrCode == -6603
	}

	return false
}
