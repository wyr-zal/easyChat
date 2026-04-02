# 联系人正则筛选导出

## 用途

把外部项目导出的微信联系人 CSV 按正则表达式筛选成微信号 txt，再导入 EasyChat 现有的“加载用户txt文件”流程中批量发送。

## 命令

```powershell
python csv_filter_contacts.py `
  --csv "contacts_2026-04-02T02-08-31.csv" `
  --pattern "陈老师|科学" `
  --output "filtered_wechat_ids.txt"
```

## 默认规则

- 默认只保留 `类型=好友`
- 默认匹配字段：`显示名称`、`备注`、`昵称`、`标签`、`详细描述`、`微信号`
- 默认导出字段：`微信号`
- 自动过滤空微信号
- 自动按微信号去重

## 常用参数

- `--csv`：联系人 CSV 路径
- `--pattern`：Python `re` 正则表达式
- `--output`：输出 txt 路径
- `--fields`：指定参与匹配的字段，英文逗号分隔
- `--contact-type`：按类型过滤，默认 `好友`，传空字符串可关闭
- `--ignore-case`：忽略大小写
- `--preview`：终端预览条数

## 示例

匹配备注或昵称中含“老师”的好友：

```powershell
python csv_filter_contacts.py `
  --csv "contacts_2026-04-02T02-08-31.csv" `
  --pattern "老师" `
  --output "filtered_wechat_ids.txt"
```

只在标签和详细描述里匹配“家长群”或“续费”：

```powershell
python csv_filter_contacts.py `
  --csv "contacts_2026-04-02T02-08-31.csv" `
  --pattern "家长群|续费" `
  --fields "标签,详细描述" `
  --output "filtered_wechat_ids.txt"
```

## 导入 EasyChat

1. 先运行脚本生成 `filtered_wechat_ids.txt`
2. 打开 EasyChat
3. 点击“加载用户txt文件”
4. 选择生成的 txt 文件
5. 按现有流程添加消息并发送

## 注意事项

- 这一步只负责筛出微信号，不负责验证 PC 微信是否一定能通过该微信号搜索到联系人
- 如果个别联系人通过微信号搜不到，后续可再补“回退显示名称/备注”的增强逻辑
