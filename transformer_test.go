package dameng

import (
	"testing"
)

func TestBooleanSyntaxTransformer(t *testing.T) {
	transformer := NewBooleanSyntaxTransformer()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "IS TRUE - 大写",
			input:    "SELECT * FROM users WHERE active IS TRUE",
			expected: "SELECT * FROM users WHERE active = true",
		},
		{
			name:     "IS FALSE - 大写",
			input:    "SELECT * FROM users WHERE active IS FALSE",
			expected: "SELECT * FROM users WHERE active = false",
		},
		{
			name:     "IS NOT TRUE - 大写",
			input:    "SELECT * FROM users WHERE active IS NOT TRUE",
			expected: "SELECT * FROM users WHERE active = false",
		},
		{
			name:     "IS NOT FALSE - 大写",
			input:    "SELECT * FROM users WHERE active IS NOT FALSE",
			expected: "SELECT * FROM users WHERE active = true",
		},
		{
			name:     "is true - 小写",
			input:    "select * from users where active is true",
			expected: "select * from users where active = true",
		},
		{
			name:     "is false - 小写",
			input:    "select * from users where active is false",
			expected: "select * from users where active = false",
		},
		{
			name:     "is not true - 小写",
			input:    "select * from users where active is not true",
			expected: "select * from users where active = false",
		},
		{
			name:     "is not false - 小写",
			input:    "select * from users where active is not false",
			expected: "select * from users where active = true",
		},
		{
			name:     "混合大小写",
			input:    "SELECT * FROM users WHERE active Is True AND deleted Is False",
			expected: "SELECT * FROM users WHERE active = true AND deleted = false",
		},
		{
			name:     "多个条件",
			input:    "SELECT * FROM users WHERE active IS TRUE AND deleted IS FALSE AND verified IS NOT TRUE",
			expected: "SELECT * FROM users WHERE active = true AND deleted = false AND verified = false",
		},
		{
			name:     "包含多余空格",
			input:    "SELECT * FROM users WHERE active  IS   TRUE",
			expected: "SELECT * FROM users WHERE active  = true",
		},
		{
			name:     "复杂查询",
			input:    "UPDATE users SET active = 1 WHERE deleted IS FALSE AND (verified IS TRUE OR admin IS NOT FALSE)",
			expected: "UPDATE users SET active = 1 WHERE deleted = false AND (verified = true OR admin = true)",
		},
		{
			name:     "JOIN查询",
			input:    "SELECT u.* FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active IS TRUE AND p.published IS FALSE",
			expected: "SELECT u.* FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = true AND p.published = false",
		},
		{
			name:     "WHERE子句中多个布尔条件",
			input:    "DELETE FROM users WHERE active IS FALSE AND deleted IS TRUE",
			expected: "DELETE FROM users WHERE active = false AND deleted = true",
		},
		{
			name:     "CASE语句中的布尔条件",
			input:    "SELECT CASE WHEN active IS TRUE THEN 'yes' ELSE 'no' END FROM users",
			expected: "SELECT CASE WHEN active = true THEN 'yes' ELSE 'no' END FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := transformer.Transform(tt.input)
			if result != tt.expected {
				t.Errorf("Transform() = %v, want %v", result, tt.expected)
			}
		})
	}
}

