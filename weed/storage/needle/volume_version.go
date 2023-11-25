package needle

type Version uint8

const (
	Version1       = Version(1)
	Version2       = Version(2)
	Version3       = Version(3) // Deprecated. 在Nebulas的Version3版本，dat文件中的Needle结构与Version2相同，但是修改了idx文件中结构，由原来的16个字节，改为了17个字节，新能1字节的Origin字段。
	Version4       = Version(4) // 修改 dat 文件中的Needle结构，在checksum后面增加一个8字节的Timestamp字段
	CurrentVersion = Version3
)
