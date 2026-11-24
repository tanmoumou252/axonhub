// 职责: FakeStreamMode 的 GraphQL 封送契约：未配置零值必须输出 null（空串是非法枚举字面量）。
// 关联文件:
//   internal/objects/channel.go
package objects

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFakeStreamModeMarshalGQL(t *testing.T) {
	var unset FakeStreamMode
	var buf strings.Builder
	unset.MarshalGQL(&buf)
	require.Equal(t, "null", buf.String(), "unset fakeStream must marshal as null")

	buf.Reset()
	FakeStreamModeFake.MarshalGQL(&buf)
	require.Equal(t, `"fake_stream"`, buf.String())

	var out FakeStreamMode
	require.NoError(t, out.UnmarshalGQL(nil))
	require.Equal(t, FakeStreamMode(""), out, "null input keeps the inherit zero value")
	require.NoError(t, out.UnmarshalGQL("placeholder"))
	require.Equal(t, FakeStreamModePlaceholder, out)
}
