# EasyChat JSON 任务字段说明

## 顶层字段

| 字段名 | 类型 | 必填 | 说明 | 示例 |
|---|---|---|---|---|
| `start_time` | `string` | 是 | 首次执行时间，格式固定为 `yyyy-MM-dd HH:mm:ss` | `2026-04-11 09:30:00` |
| `end_time` | `string` | 否 | 本轮任务真正结束时间；一次性任务执行完成后会回写，周期任务通常保持空 | `2026-04-11 09:42:18` |
| `schedule_mode` | `string` | 否 | 任务频率，支持 `once` / `daily` / `weekly` / `cron`，默认 `once` | `cron` |
| `schedule_value` | `string` | 否 | 频率补充参数；仅 `schedule_mode=cron` 时必填 | `0 9 * * 1-5` |
| `total_count` | `number` | 否 | 发送目标总数；导入时会以实际 `targets` 为准做校验 | `3` |
| `template_content` | `string` | 否 | 模板消息内容；当目标使用 `message_mode=template` 时生效 | `您好，{{姓名}}` |
| `common_attachments` | `array` | 否 | 通用附件列表；所有目标都可共用 | 见下方附件结构 |
| `targets` | `array` | 是 | 发送目标列表，至少 1 条 | 见下方目标结构 |

## 顶层频率字段说明

| `schedule_mode` | 含义 | `schedule_value` 要求 |
|---|---|---|
| `once` | 一次性任务 | 留空即可 |
| `daily` | 每天重复 | 留空即可 |
| `weekly` | 每周重复 | 留空即可 |
| `cron` | 自定义 Cron 表达式 | 必填，5 段格式：分 时 日 月 周 |

## `common_attachments[]` / `targets[].attachments[]` 附件结构

| 字段名 | 类型 | 必填 | 说明 | 示例 |
|---|---|---|---|---|
| `file_path` | `string` | 是 | 本地文件绝对路径或可解析相对路径 | `D:\资料\合同.zip` |
| `file_type` | `string` | 否 | 附件展示类型；兼容旧值 `pdf` / `image`，新增通用值 `file` | `file` |

说明：
- 现在支持任意本地文件，不再只限制 PDF / 图片。
- 旧 JSON 中的 `pdf` / `image` 仍然兼容。

## `targets[]` 目标结构

| 字段名 | 类型 | 必填 | 说明 | 示例 |
|---|---|---|---|---|
| `target_value` | `string` | 是 | 微信搜索关键词/发送目标 | `张三` |
| `target_type` | `string` | 是 | 目标类型：`person` / `group` | `person` |
| `message_mode` | `string` | 是 | 消息模式：`template` / `custom` | `custom` |
| `message` | `string` | 条件必填 | `message_mode=custom` 时必填；模板模式可留空 | `请查收附件` |
| `attachment_mode` | `string` | 是 | 附件模式：`common` / `custom` | `custom` |
| `attachments` | `array` | 否 | 当前目标的自定义附件；`attachment_mode=common` 时通常为空 | 见附件结构 |
| `display_name` | `string` | 否 | 目标显示名，用于预览与记录 | `张三（家长群）` |
| `send_status` | `string` | 否 | 当前目标发送状态；常见值：`pending` / `success` / `failed` / `skipped` | `failed` |
| `attachment_status` | `string` | 否 | 当前目标附件状态；常见值：`none` / `success` / `failed` / `skipped` | `success` |
| `error_msg` | `string` | 否 | 失败原因或调试说明 | `模拟发送失败` |
| `send_time` | `string` | 否 | 当前目标最后一次执行时间 | `2026-04-11 09:31:05` |
| `source_target_index` | `number` | 否 | 原始目标顺序；用于回写和续跑定位 | `2` |

## 兼容说明

1. **旧附件类型兼容**
   - 旧 JSON 里的 `file_type=pdf` / `image` 可以继续导入。
   - 新版本允许 `file_type=file`，也允许省略后由系统推断。

2. **旧一次性任务兼容**
   - 如果缺少 `schedule_mode` / `schedule_value`，系统按一次性任务 `once` 处理。

3. **失败后继续发送**
   - 某个目标失败后，任务会立即停止。
   - 后续点击“继续发送”时，默认只继续失败项后面的未发送目标。
   - 失败目标需要用户手动处理，不会自动重试。

## 示例

```json
{
  "start_time": "2026-04-11 09:30:00",
  "end_time": "",
  "schedule_mode": "cron",
  "schedule_value": "0 9 * * 1-5",
  "total_count": 2,
  "template_content": "您好，{{姓名}}",
  "common_attachments": [
    {
      "file_path": "D:\\资料\\通知.zip",
      "file_type": "file"
    }
  ],
  "targets": [
    {
      "target_value": "张三",
      "target_type": "person",
      "message_mode": "template",
      "message": "",
      "attachment_mode": "common",
      "attachments": [],
      "display_name": "张三",
      "send_status": "pending",
      "attachment_status": "none",
      "error_msg": "",
      "send_time": "",
      "source_target_index": 1
    },
    {
      "target_value": "高三1班",
      "target_type": "group",
      "message_mode": "custom",
      "message": "请查收本周资料",
      "attachment_mode": "custom",
      "attachments": [
        {
          "file_path": "D:\\资料\\周报.docx",
          "file_type": "file"
        }
      ],
      "display_name": "高三1班",
      "send_status": "pending",
      "attachment_status": "none",
      "error_msg": "",
      "send_time": "",
      "source_target_index": 2
    }
  ]
}
```
