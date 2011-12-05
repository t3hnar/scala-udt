package ua.t3hnar.udt

import com.barchart.udt.{TypeUDT, SocketUDT}
import java.net.InetSocketAddress
import java.nio.ByteBuffer

/**
 * @author Yaroslav Klymko
 */
class Socket(private val socket: SocketUDT = new SocketUDT(TypeUDT.DATAGRAM)) {

  import Socket._

  private def chain(f: => Any): Socket = {
    f;
    this
  }

  def bind(address: InetSocketAddress) = chain {
    socket.bind(address)
  }

  def getAddress: InetSocketAddress = socket.getLocalSocketAddress

  def connect(address: InetSocketAddress) = chain {
    socket.connect(address)
  }

  def accept: Socket = new Socket(socket.accept)

  def listen(queueSize: Int = 10) = chain {
    socket.listen(queueSize)
  }

  def close() {
    socket.close()
  }


//  implicit def intWithBytes(i: Int) = new {
//    def bytes: Bytes = ByteBuffer.wrap(new Bytes(sizeSize)).putInt(i).array()
//  }

//  implicit def bs2Int(bs: Bytes) = new {
//    def getInt: Int = ByteBuffer.wrap(bs).getInt
//  }

  def send(bs: Bytes) {
    socket.send(bs)
  }

//  private def withSize(buf: ByteBuffer): ByteBuffer = {
//    val size = buf.remaining()
//    //TODO Need to try omit this copy somehow
//    val result = ByteBuffer.allocateDirect(sizeSize + size)
//      .putInt(size)
//      .put(buf)
//    result.rewind()
//    result
//  }

  def send(buf: ByteBuffer) {
    if (buf.isDirect) socket.send(buf)
    else send(buf.array())
  }

//  def receive(implicit bufSize: Int): Bytes = {
//    val buf = new Bytes(bufSize + sizeSize)
//    socket.receive(buf)
//    val (size, data) = buf.splitAt(sizeSize)
//    data.take(size.getInt)
//  }

  def receive(implicit bufSize: Int): Bytes = {
    val buf = receiveBuffer(bufSize)
    val bs = new Bytes(buf.limit())
    buf.get(bs)
    bs
  }

  def receiveBuffer(implicit bufSize: BufSize): ByteBuffer = {
    val allocated = ByteBuffer.allocateDirect(bufSize)
    socket.receive(allocated)
    allocated.limit(allocated.position())
    allocated.rewind()
    allocated.slice()
  }

  override def toString = "Socket(" + getAddress + ")"
}

object Socket {
  type Bytes = Array[Byte]
  type BufSize = Int
  implicit val bufSize = 1024
}