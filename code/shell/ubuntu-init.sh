#!/bin/bash

# 设置错误时退出
set -e

echo "开始 Ubuntu 服务器初始化配置..."

# 第一部分：系统基础配置
echo "===== 第一部分：系统基础配置 ====="

# 更新系统
echo "正在更新系统..."
sudo apt update

# 配置系统限制
echo "正在配置系统限制..."
sudo tee -a /etc/security/limits.conf << EOF
* soft nofile 1000000
* hard nofile 1000000
EOF

# 应用系统参数
sudo sysctl -p

# 第二部分：Shell环境配置
echo "===== 第二部分：Shell 环境配置 ====="

# 安装zsh
echo "正在安装 zsh..."
sudo apt install -y zsh
chsh -s $(which zsh)

# 安装 Oh My Zsh
echo "正在安装Oh My Zsh..."
# 使用国内镜像源
REMOTE=https://gitee.com/mirrors/oh-my-zsh.git
git clone -c core.eol=lf -c core.autocrlf=false \
    -c fsck.zeroPaddedFilemode=ignore \
    -c fetch.fsck.zeroPaddedFilemode=ignore \
    -c receive.fsck.zeroPaddedFilemode=ignore \
    --depth=1 "$REMOTE" ~/.oh-my-zsh || {
    echo "Error: git clone of oh-my-zsh repo failed"
    exit 1
}

# 复制配置文件
cp ~/.oh-my-zsh/templates/zshrc.zsh-template ~/.zshrc

# 安装 zsh插件（使用国内镜像源）
echo "正在安装zsh插件..."
git clone https://gitee.com/mirrors/zsh-autosuggestions.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions || {
    echo "Error: Failed to clone zsh-autosuggestions"
    exit 1
}
git clone https://gitee.com/mirrors/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting || {
    echo "Error: Failed to clone zsh-syntax-highlighting"
    exit 1
}

# 配置 zsh 插件
sed -i 's/plugins=(git)/plugins=(git zsh-autosuggestions zsh-syntax-highlighting)/' ~/.zshrc

# 第三部分：开发环境配置
echo "===== 第三部分：开发环境配置 ====="

# 配置 Rust 环境
echo "正在配置 Rust 环境..."
echo 'export RUSTUP_DIST_SERVER="https://rsproxy.cn"' >> ~/.zshrc
echo 'export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"' >> ~/.zshrc

# 安装 Rust
echo "正在安装 Rust..."
curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh -s -- -y

# 配置 Cargo 镜像
mkdir -p ~/.cargo
cat > ~/.cargo/config << EOF
[source.crates-io]
replace-with = 'rsproxy-sparse'
[source.rsproxy]
registry = "https://rsproxy.cn/crates.io-index"
[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"
[registries.rsproxy]
index = "https://rsproxy.cn/crates.io-index"
[net]
git-fetch-with-cli = true
EOF

# 安装 protoc 和 atuin
echo "正在安装 protoc 和 atuin..."
sudo apt install -y protobuf-compiler
source "$HOME/.cargo/env"
cargo install atuin

# 配置 atuin
echo 'eval "$(atuin init zsh)"' >> ~/.zshrc

# 第四部分：应用服务配置
echo "===== 第四部分：应用服务配置 ====="

# 安装 MySQL
echo "正在安装 MySQL..."
sudo apt install -y mysql-server-8.0

# 启动 MySQL 服务
echo "正在配置 MySQL 服务..."
sudo systemctl start mysql
sudo systemctl enable mysql

# 配置 MySQL 远程访问
sudo sed -i 's/bind-address.*=.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf
sudo systemctl restart mysql

# 最后配置系统参数
echo "正在配置系统参数..."
echo "ulimit -n 65536" >> ~/.zshrc
echo "sysctl -w vm.max_map_count=2000000" >> ~/.zshrc

# 创建软件和模块目录
echo "正在创建软件和模块目录..."
sudo mkdir -p /opt/software
sudo mkdir -p /opt/module