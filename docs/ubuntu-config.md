# Ubuntu 服务器初始化配置

## zsh

### 1.安装 zsh：

```Bash
sudo apt update
sudo apt install zsh
```

### 2.更改默认 shell 为 zsh

```bash
chsh -s $(which zsh)
```

### 3.安装 Oh My Zsh

```Bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"
```

### 4.安装插件

```zsh
git clone https://github.com/zsh-users/zsh-autosuggestions.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting
```

```zsh
vim ~/.zshrc
plugins=(git zsh-autosuggestions zsh-syntax-highlighting)
```

## Rust

### 1.设置 Rustup 镜像，修改配置 ~/.zshrc or ~/.bashrc

```zsh
export RUSTUP_DIST_SERVER="https://rsproxy.cn"
export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
```

### 2.安装 Rust（请先完成步骤一的环境变量导入并 source rc 文件或重启终端生效）

curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh

### 3.设置 crates.io 镜像，修改配置 ~/.cargo/config，已支持 git 协议和 sparse 协议，>=1.68 版本建议使用 sparse-index，速度更快。

```zsh
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
```

## Atuin（便于管理命令）

### 1.安装 protoc

```zsh
sudo apt install protobuf-compiler

protoc --version
```

### 2.安装 atuin

```zsh
cargo intsall atuin
```

### 3.添加环境变量

```zsh
echo 'eval "$(atuin init zsh)"' >> ~/.zshrc

source ~/.zshrc
```

## MySQLn 8.0

### 1.安装

```zsh
sudo apt update
sudo apt install -y mysql-server-8.0
```

### 2.重启 MySQL 服务

```zsh
# 启动
sudo systemctl start mysql
# 开机自启
sudo systemctl enable mysql
# 查看状态
sudo systemctl status mysql
```

### 3.修改密码权限

默认安装是没有设置密码的，需要我们自己设置密码。

```zsh
# 登录 mysql，在默认安装时如果没有让我们设置密码，则直接回车就能登录成功。
mysql -uroot -p
# 设置密码 mysql8.0
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '新密码';
# 设置密码 mysql5.7
set password=password('新密码');
# 配置 IP 5.7
grant all privileges on *.* to root@"%" identified by "密码";
# 刷新缓存
flush privileges;
```

开发 IP 访问。

```zsh
sudo vim /etc/mysql/mysql.conf.d/mysqld.cnf

bind-address = 0.0.0.0
```

```zsh
sudo systemctl restart mysql
```

## 打开最大文件数量

```zsh
vim ~/.zshrc

ulimit -n 65535
sysctl -w vm.max_map_count=2000000

source ~/.zshrc
```

检测到最大打开文件数小于 65536，建议修改最大打开文件数为 1000000，请打开终端窗口，输入以下命令 (需要重启终端及 Agent 生效)：

```zsh
sudo vim /etc/security/limits.conf
* soft nofile 1000000
* hard nofile 1000000
sysctl -p

reboot
```
