package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	//第一部分为diskQueue当前读写的文件的状态
	readPos      int64 //表示当前应该从readFileNum这个序号的文件的readPos位置开始读取数据
	writePos     int64 //表示当前应该从writeFileNum这个序号的文件的writePos位置开始写入数据
	readFileNum  int64 //表示当前应该从readFileNum这个序号的文件中读取数据
	writeFileNum int64 //表示当前应该向writeFile这个序号的文件中写入数据。
	depth        int64 //diskQueue中当前可供读取或消费的消息的数量

	sync.RWMutex

	// instantiation time metadata
	//第二部分为diskQueue的元数据信息,这部分的信息实在实例化topic和channel的时候就会被填充
	name            string // diskQueue 名称
	dataPath        string // 数据持久化路径
	maxBytesPerFile int64 // 单个文件最大大小。此此属性一旦被初始化，则不可变更。
	minMsgSize      int32 // 最小消息的大小
	maxMsgSize      int32 // 最大消息的大小
	syncEvery       int64         // number of writes per fsync，每写多少条消息需要执行刷盘操作
	syncTimeout     time.Duration // duration of time per fsync，// 两次sync之间的间隔
	exitFlag        int32  // 退出标志
	needSync        bool // 是否需要同步刷新

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	// 之所以存在 nextReadPos & nextReadFileNum 和 readPos & readFileNum,
	// 是因为虽然消费者已经发起了数据读取请求，但 diskQueue 还未将此消息发送给消费者，
	// 当发送完成后，会将 readPos 更新到 nextReadPos，readFileNum 也类似
	nextReadPos     int64 // 下一个应该被读取的索引位置
	nextReadFileNum int64 // 下一个应该被读取的文件号
	//第三部分是读写文件句柄
	readFile  *os.File // 当前读取文件句柄
	writeFile *os.File// 当前写入文件句柄
	reader    *bufio.Reader// 当前文件读取流
	writeBuf  bytes.Buffer// 当前文件写入流
	//最后一部分为用于传递信号的内部管道
	// exposed via ReadChan()
	// 应用程序可通过此通道从 diskQueue中读取消息，
	// 因为 readChan 是 unbuffered的，所以，读取操作是同步的
	// 另外当一个文件中的数据被读取完时，文件会被删除，同时切换到下一个被读取的文件
	readChan chan []byte

	// internal channels
	writeChan         chan []byte  // 应用程序通过此通道往 diskQueue压入消息，写入操作也是同步的
	writeResponseChan chan error // 可通过此通道向应用程序返回消息写入结果
	emptyChan         chan int // 应用程序可通过此通道发送清空 diskQueue 的消息
	emptyResponseChan chan error // 可通过此通道向应用程序返回清空 diskQueue 的结果
	exitChan          chan int // 退出信号
	exitSyncChan      chan int // 保证 ioLoop 已退出的信号

	logf AppLogFunc
}

//在实例化topic或channel时候,会实例化diskQueue
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	//1.根据传入的参数构造实例，此时传入的参数主要是diskQueue的元数据信息，即结构体中的第二部分信息
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	//2.从配置文件中加载diskQueue的重要的属性状态，这包括readPos & writePos,readFileNum & writerFileNum和depth，
	// 并初始化nextReadFileNum和nextReadPos两个重要的属性。即结构体中的第一部分信息。
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}
	// 3. 主循环
	go d.ioLoop()
	return &d
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
// 获取 diskQueu 的读取通道，即 readChan，通过此通道从 diskQueue 中读取/消费消息
func (d *diskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
// 清空 diskQueue 中未读取的文件
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}
// 删除目前还未读取的文件，同时删除元数据文件
func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile() //清空 readFileNum -> writeFileNum 之间的文件，并且设置 depth 为 0。

	innerErr := os.Remove(d.metaDataFileName())//同时删除元数据文件
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0) //设置depth为0

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
//核心逻辑为：若当前持久化中还有未被读取或消费的消息，则尝试从特定的文件(readFileNum)、特定偏移位置(readPos)读取一条消息。
//值得注意的一点是：程序中还使用了另外一组与读取相关的状态(nextReadFileNum和nextReadPos)。
// 当消息未从文件中读取时，readPos == nextReadPos && readFileNum == nextReadFileNum ，
// 当消息已从文件中读出但未发送给应用程序时，readPos + totalBytes == nextReadPos && readFileNum == nextReadFileNum（若涉及到文件切换，则nextReadFileNum++ && nextReadPos == 0），
// 当消息已经发送给应用程序时，readPos == nextReadPos && readFileNum == nextReadFileNum。换言之，之所以存在nextReadFileNum和nextReadPos是因为虽然消费者已经发起了数据读取请求，
// 但 diskQueue还未将此消息发送给消费者，当发送完成后，会将它们相应更新。好，文件读取过程已经阐述完毕。当消息从文件中读取出来后，是通过diskQueue.readChan发送给上层应用程序的，
// 上层应用程序通过调用diskQueue.ReadChan获取到此管道实例，并一直等待从此管道接收消息。
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)//根据文件编号拿到文件名
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	// 当消息未从文件中读取时，readPos == nextReadPos && readFileNum == nextReadFileNum ，
	// 当消息已从文件中读出但未发送给应用程序时，readPos + totalBytes == nextReadPos && readFileNum == nextReadFileNum（若涉及到文件切换，则nextReadFileNum++ && nextReadPos == 0）。
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
// 将一个字节数组内容写入到持久化存储，同时更新读写位置信息，以及判断是否需要滚动文件。
func (d *diskQueue) writeOne(data []byte) error {
	var err error
	// 1. 若当前写入文件句柄为空，则需要先实例化
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)
		// 2. 同时，若当前的写入索引大于0,则重新定位写入索引
		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}
	// 3. 获取写入数据长度，并检查长度合法性。然后将数据写入到写入缓冲，最后将写入缓冲的数据一次性刷新到文件
	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}
	// 更新写入索引 writePos 及 depth，且若 writePos 大于 maxBytesPerFile，则说明当前已经写入到文件的末尾。
	// 因此需要更新 writeFileNum，重置 writePos，即更换到一个新的文件执行写入操作（为了避免一直写入单个文件），
	// 且每一次更换到下一个文件，都需要将写入文件同步到磁盘
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
//diskQueue 刷盘操作
// 同步刷新 writeFile文件流（即将操作系统缓冲区中的数据写入到磁盘），同时持久化元数据信息
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync() //批量刷新刷盘动作助于提高文件系统的写入性能。
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false // 重置了刷新开关
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error
	// 1. 获取元数据文件名 *.diskqueue.meta.dat，并打开文件，准备读取文件
	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	// 2. 从文件中内容初始化特定状态属性信息 readPos, writerPos, depth
	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth, ///diskQueue中当前可供读取的消息的数量
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	// 3. 初始化 nextReadFileNum 和 nextReadPos
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}
// 检测文件末尾是否已经损坏
func (d *diskQueue) checkTailCorruption(depth int64) {
	// 若当前还有消息可供读取，则说明未读取到文件末尾，暂时不用检查
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	// 若上面没有return，则正常情况下，说明已经读取到 diskQueue 的尾部，
	// 即读取到了最后一个文件的尾部了，因此，此时的 depth(累积等待读取或消费的消息数量)
	// 应该为0,因此若其不为0,则表明文件尾部已经损坏，报错。
	// 一方面，若其小于 0,则表明初始化加载的元数据已经损坏（depth从元数据文件中读取而来）
	// 原因是：实际上文件还有可供读取的消息，但depth指示没有了，因此 depth 计数错误。
	// 否则，说明是消息实体数据存在丢失的情况
	// 原因是：实际上还有消息可供读取 depth > 0,但是文件中已经没有消息了，因此文件被损坏。
	// 同时，强制重置 depth，并且设置 needSync
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)
		d.needSync = true
	}
	// 另外，若 depth == 0，即此时所有的消息已经被读取完毕，但若某个异常的情况下，可能会有
	// 文件读取记录信息不合法 d.readFileNum != d.writeFileNum || d.readPos != d.writePos，
	// 则会显式地删除掉接下来需要被读或写的所有文件，类似于重置持久化存储的状态或格式化操作。
	// 同时设置 needSync
	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()//skipToNextRWFile也可用作清空 diskQueue当前未读取的所有文件。
		d.needSync = true
	}
}
// 检查当前读取的文件和上一次读取的文件是否为同一个，即读取是否涉及到文件的更换，
// 若是，则说明可以将磁盘中上一个文件删除掉，因为上一个文件包含的消息已经读取完毕，
// 同时需要设置 needSync
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1)

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		// 每当准备读取一个新的文件时，需要设置 needSync
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn) // 将老的文件删除
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}

	d.checkTailCorruption(depth) // 检测文件末尾是否已经损坏，它先通过元数据信息（4个变量）判断是否已经读到了最后一个文件的末尾，若未到，则返回。否则，通过depth与0的大小关系来判断文件损坏的类型或原因。
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
//diskQueue 消息处理主循环，在上述结构体被实例化的时候会开始执行。
//整个diskqueue被封装起来了，暴露给外面的只有几个公有函数New,Put,Delete等来操作。
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		//同大多的存储系统类似，diskQueue采用批量刷新缓冲区的操作来提高消息写入文件系统的性能。
		// 其中，diskQueue规定触发刷盘动作的有2个条件，其中任一条件成立即可。
		// 一是当缓冲区中的消息的数量达到阈值(syncEvery)时，二是每隔指定时间(syncTimeout)。
		// 需要注意的一点为在执行刷盘动作时，也会重新持久化diskQueue的元数据信息。
		if count == d.syncEvery { // 1. 只有写入缓冲中的消息达到一定数量，才执行同步刷新到磁盘的操作
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()// 2. 刷新磁盘操作，重置计数信息，即将 writeFile 流刷新到磁盘，同时持久化元数据
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0 // 重置当前等待刷盘的消息数量
		}
		// 3. 从文件中读取消息的逻辑
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {//若磁盘还有（消息）可供读取
			//当nextReadPos != readPos的时候，说明readChan中的消息还没被读走。不用新从文件readOnce
			//当nextReadPos == readPos的时候，说明readChan中的消息已经被读走了，需要重新从文件readOnce一条消息
			//因为在下面的case r <- dataRead情况下，若阻塞解除，则表示readChan中的值被读走，则接下来moveForward会使nextReadPos == readPos
			if d.nextReadPos == d.readPos { //若当前持久化中还有未被读取或消费的消息，则调用readOne，尝试从特定的文件(readFileNum)、特定偏移位置(readPos)读取一条消息。
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan // 取出读取通道 readChan
		} else {
			// 当 r == nil时，代表此时消息已经全部读取完毕，
			// 因此使用 select 不能将数据（消息）压入其中
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		// 4. 当读取到数据时，将它压入到 r/readChan 通道，
		// 同时判断是否需要更新到下一个文件读取，同时设置 needSync
		case r <- dataRead:
			count++ // 更新当前等待刷盘的消息数量
			// moveForward sets needSync flag if a file is removed
			// moveForward判断是否可以将磁盘中读取的上一个文件删除掉（已经读取完毕），同时需要设置 needSync，如果一个文件被删除了，moveForward会设置needSync标志位。
			// 值得注意的是，moveForward 方法中将 readPos 更新为了 nextReadPos，
			// 且 readFileNum 也被更新为 nextReadFileNum
			// 因为此时消息已经发送给了消费者了。
			d.moveForward()
		case <-d.emptyChan: // 5. 收到清空“持久化存储disQueue中的消息”，(当应用程序调用 diskQueue.Empty方法时触发此处)
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan: // 6. 收到写入消息到磁盘的消息 (当应用程序调用 diskQueue.Put方法时触发)，消息写入持久化存储的逻辑比从文件系统中读取一条消息的逻辑要简单。
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C: // 7. 定时执行刷盘操作，在存在数据等待刷盘时，才需要执行刷盘动作
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		case <-d.exitChan: // 8. 退出信号
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
