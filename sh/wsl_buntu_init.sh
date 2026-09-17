
#查看当前占用大小
df -h /
sudo apt update

sudo apt install htop

sudo apt install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    curl \
    zip \
    unzip \
    tar \
    pkg-config

cd ~

git clone https://github.com/microsoft/vcpkg.git

cd ~/vcpkg

./bootstrap-vcpkg.sh

echo 'export VCPKG_ROOT="$HOME/vcpkg"' >> ~/.bashrc
echo 'export PATH="$VCPKG_ROOT:$PATH"' >> ~/.bashrc

source ~/.bashrc

# pkg-config是辅助开发工具，帮编译器找到某个库的头文件和链接参数。
sudo apt install -y pkg-config
# Autotools 工具链
sudo apt install -y autoconf autoconf-archive automake libtool


#打开到工程目录
vcpkg install

cmake -B build -S .   -DCMAKE_TOOLCHAIN_FILE=/home/nsyl/vcpkg/scripts/buildsystems/vcpkg.cmake

# 打开clion设置 CMake options： -DCMAKE_TOOLCHAIN_FILE=/home/nsyl/vcpkg/scripts/buildsystems/vcpkg.cmake