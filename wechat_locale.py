

"""
This class defines the mapping of the WeChat UI elements to the English and Chinese names.
这个类定义了微信程序 UI 元素到英文和中文名称的映射。
"""
class WeChatLocale:
    MAPPING = {
        "weixin":       {"en-US": "Weixin",         "zh-CN": "微信",            "zh-TW": "微信"},

        "chats":        {"en-US": "Chats",          "zh-CN": "聊天",            "zh-TW": "聊天"},
        "contacts":     {"en-US": "Contacts",       "zh-CN": "通讯录",          "zh-TW": "通訊錄"},
        "facorites":    {"en-US": "Favorites",      "zh-CN": "收藏",            "zh-TW": "收藏"},
        "chat_files":   {"en-US": "Chat Files",     "zh-CN": "聊天文件",        "zh-TW": "聊天室檔案"},
        "moments":      {"en-US": "Moments",        "zh-CN": "朋友圈",          "zh-TW": "朋友圈"},
        "channels":     {"en-US": "Channels",       "zh-CN": "视频号",          "zh-TW": "影音號"},
        "mini_programs_panel":  {"en-US": "Mini Programs Panel", "zh-CN": "小程序面板", "zh-TW": "小程式面板"},
        "phone":        {"en-US": "Phone",          "zh-CN": "手机",            "zh-TW": "手機"},
        "settings_and_others":  {"en-US": "Settings and Others", "zh-CN": "设置及其他", "zh-TW": "設定與其他"},
        
        "search":       {"en-US": "Search",         "zh-CN": "搜索",            "zh-TW": "搜尋"},
        "send":         {"en-US": "Send (S)",       "zh-CN": "发送(S)",         "zh-TW": "傳送（S）"},
        "contact":      {"en-US": "contact",        "zh-CN": "通讯录",          "zh-TW": "通訊錄"},
        "group_chat":   {"en-US": "Group Chat",     "zh-CN": "群聊",            "zh-TW": "群聊"},
        "manage_contacts":  {"en-US": "Manage Contacts", "zh-CN": "通讯录管理", "zh-TW": "通訊錄管理"},

        "message":      {"en-US": "消息",           "zh-CN": "消息",            "zh-TW": "消息"},
        "chat_history": {"en-US": "Chat History",   "zh-CN": "聊天记录",        "zh-TW": "聊天記錄"},
        "photos_n_videos":  {"en-US": "Photos & Videos", "zh-CN": "图片与视频", "zh-TW": "圖片與影片"},
        "copy":      {"en-US": "Copy",              "zh-CN": "复制",            "zh-TW": "複製"},

        "quick_action":     {"en-US": "Quick Action",      "zh-CN": "快捷操作",        "zh-TW": "快捷操作"},
        "initiate_group":   {"en-US": "Start Group Chat",  "zh-CN": "发起群聊",        "zh-TW": "發起群聊"},
        "chat_info":        {"en-US": "Chat Info",         "zh-CN": "聊天信息",        "zh-TW": "聊天資訊"},
        "done":             {"en-US": "Done",              "zh-CN": "完成",            "zh-TW": "完成"},
        "remove":           {"en-US": "Remove",            "zh-CN": "移出",            "zh-TW": "移出"},
        "cancel":           {"en-US": "Cancel",            "zh-CN": "取消",            "zh-TW": "取消"},
        "exit_group":       {"en-US": "Exit Group",        "zh-CN": "退出群聊",        "zh-TW": "退出群聊"},
        "chat_members":     {"en-US": "Chat Members",      "zh-CN": "聊天成员",        "zh-TW": "聊天成員"},
    }

    """
    @param locale: the locale of the WeChat UI, either "en-US", "zh-CN", or "zh-TW"
    """
    def __init__(self, locale="en-US"):
        for key, value in WeChatLocale.MAPPING.items():
            setattr(self, key, value[locale])
    
    @staticmethod
    def getSupportedLocales():
        return list(WeChatLocale.MAPPING.values())[0].keys()


if __name__ == "__main__":
    print(WeChatLocale.getSupportedLocales())

    lc = WeChatLocale("zh-CN")

