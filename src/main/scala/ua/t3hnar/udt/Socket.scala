package ua.t3hnar.udt

import com.barchart.udt.{TypeUDT, SocketUDT}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import scala.Predef._

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


  implicit def intWithBytes(i: Int) = new {
    def bytes: Bytes = ByteBuffer.wrap(new Bytes(sizeSize)).putInt(i).array()
  }

  implicit def bs2Int(bs: Bytes) = new {
    def getInt: Int = ByteBuffer.wrap(bs).getInt
  }

  def send(bs: Bytes) {
    val size = bs.length.bytes
    val res = size ++ bs
    socket.send(res)
  }

  private def withSize(buf: ByteBuffer): ByteBuffer = {
    val size = buf.remaining()
    //TODO !!!
    val result = ByteBuffer.allocateDirect(sizeSize + size)
      .putInt(size)
      .put(buf)
    result.rewind()
    result
  }


  def send(buf: ByteBuffer) {
    if (buf.isDirect) socket.send(withSize(buf))
    else send(buf.array())
  }


//  def receive(implicit bufSize: Int): Bytes = {
//    val buf = new Bytes(bufSize + sizeSize)
//    socket.receive(buf)
//    val (size, data) = buf.splitAt(sizeSize)
//    data.take(size.getInt)
//  }

  def receive(implicit bufSize: Int): Bytes = {
    val (buf, _) = receiveBuffer(bufSize)
    val bs = new Bytes(buf.limit())
    buf.get(bs)
    bs
  }

  def receiveBuffer(implicit bufSize: BufSize): (ByteBuffer, BufSize) = {
    val allocated = ByteBuffer.allocateDirect(bufSize + sizeSize)
    socket.receive(allocated)
    allocated.limit(allocated.position())
    allocated.rewind()

    val withSize = allocated.slice()
    val sentSize = withSize.getInt

    val buf = withSize.slice()
    buf.rewind()
    buf -> sentSize
  }

  override def toString = "Socket(" + getAddress + ")"
}

object Socket {
  type Bytes = Array[Byte]
  type BufSize = Int
  val sizeSize = 4
  implicit val bufSize = 1024
}