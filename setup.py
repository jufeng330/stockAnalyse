from setuptools import setup, find_packages
import pathlib

# 读取 README.md 作为包描述
here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    # 包名（pip install 时的名称，建议小写，无空格）
    name="stockAnalyse",  # 注意：pip 安装时会自动转小写，建议直接用 stockanalyse
    # 版本号（遵循 semver：主版本.次版本.修订号）
    version="0.1.0",
    # 简短描述
    description="Stock analysis library with stocklib, utils and openclaw_skills",
    # 详细描述（读取 README）
    long_description=long_description,
    long_description_content_type="text/markdown",
    # 作者信息（按需修改）
    author="jujinbu",
    author_email="da_bu@yeah.net",
    # Python 版本要求
    python_requires=">=3.7",
    # 依赖包（从 requirements.txt 读取，或直接列）
    install_requires=[
        # 示例：根据你的 requirements.txt 填写，比如
        # "pandas>=1.0",
        # "requests>=2.20",
        # 若要自动读取 requirements.txt，可替换为：
        # open("requirements.txt").read().splitlines()
    ],
    # 自动发现 src 下的新分层包
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    # 包内数据文件（若有静态文件/配置需包含）
    include_package_data=True,
    # 包的分类（可选，上传 PyPI 时用）
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)