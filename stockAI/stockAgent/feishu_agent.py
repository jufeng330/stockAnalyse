import markdown
import requests
from pathlib import Path
import os

class FeishuAgent:
    def __init__(self, app_id = 'cli_a894420e98fc901c',app_token='PdKodcAzvcKbwD2uw22rkdOhCXnrnTku'):
        self.app_id = app_id
        self.app_token = app_token
        self.folder_id = 'Kt0pf15dEl53lmd3SyAcNVIEneb'

    def read_md_file(self,file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            md_content = file.read()
        html_content = markdown.markdown(md_content)
        return html_content

    def upload_file_to_feishu_doc(self, file_path):
        html_content = self.read_md_file(file_path)
        # 获取完整文件名（包含扩展名）
        full_filename = os.path.basename(file_path)  # 输出："local_file.txt"
        # 获取文件名（不含扩展名）
        filename_without_ext = os.path.splitext(full_filename)[0]  # 输出："local_file"
        # 获取文件扩展名（包含点号）
        file_extension = os.path.splitext(full_filename)[1]  # 输出：".txt"
        title = filename_without_ext
        self.create_feishu_doc(title,html_content)
    def create_feishu_doc(self,title, html_content):
        url = "https://open.feishu.cn/open-apis/drive/v1/docs/create"
        headers = {
            "Authorization": f"Bearer {self.app_token}",
            "Content-Type": "application/json"
        }
        data = {
            "title": title,
            "content": html_content,
            "parent_type": "folder",
            "parent_token": self.folder_id
        }
        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                print("文档创建成功")
                print(response.json())
            else:
                print(f"文档创建失败，状态码: {response.status_code}")
                print(response.text)

        except Exception as e:
            print(f"请求发生错误: {e}")

    def upload_path_to_feishu_doc(self, directory):
        """处理目录下所有 .txt 文件"""
        txt_files = []

        # 遍历目录，收集所有 .txt 文件
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.txt'):
                    txt_files.append(Path(root) / file)

        if not txt_files:
            print(f"目录 '{directory}' 中未找到 .txt 文件")
            return

        print(f"找到 {len(txt_files)} 个 .txt 文件")

        # 逐个处理文件
        for file_path in txt_files:
            try:
                result = self.upload_file_to_feishu_doc(file_path)
                if result:
                    print(f"已创建文档: {file_path}")
            except Exception as e:
                print(f"处理文件 '{file_path}' 时出错: {str(e)}")


if __name__ == "__main__":
    import requests  # 确保导入 requests 库

    agent = FeishuAgent()
    directory = "/Users/jujinbu/PycharmProjects/StockAnalyse/stock_analyse/stockAI/stockAgent/result"

    agent.upload_path_to_feishu_doc(directory)
    print("End")