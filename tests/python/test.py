import os
from pymemcache.client.base import Client

os.environ["PYTHON_COLORS"] = "0"

# 连接本地 memcached，默认端口 11211
# 建议设置 connect_timeout 和 timeout，避免网络问题导致阻塞[citation:16]
client = Client(('127.0.0.1', 2333), connect_timeout=1, timeout=0.5, default_noreply=False)

# 写入缓存，expire=60 表示 60 秒后过期
client.set('my_key', 'Hello, memcached!')

# 读取缓存，注意返回的是 bytes 类型[citation:16]
value = client.get('my_key')
if value:
    print(f"获取到的值: {value.decode('utf-8')}")
else:
    print("键不存在或已过期")

# 删除键 未适配
# client.delete('my_key')
print("删除后获取:", client.get('my_key'))

# 关闭连接
# client.close()
