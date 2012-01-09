package ua.t3hnar.udt

import org.specs2.mutable.SpecificationWithJUnit
import actors.Actor._
import java.net.InetSocketAddress
import util.Random
import Socket._
import java.nio.ByteBuffer

/**
 * @author Yaroslav Klymko
 */
class SocketSpec extends SpecificationWithJUnit {

  implicit def bytesWithMd5(bs: Bytes) = new {
    def md5: String = DigestUtils.md5Hex(bs)
  }

  implicit def bufWithMd5(buf: ByteBuffer) = new {
    def md5: String = DigestUtils.md5Hex {
      if (buf.isDirect) {
        buf.rewind()
        val bs = new Bytes(buf.limit())
        buf.get(bs)
        buf.rewind()
        bs
      } else buf.array()
    }
  }


  val server = new Socket().bind(new InetSocketAddress("localhost", 12345)).listen()
  val size = 1000
  val bytes = randomBytes(size)
  val md5 = bytes.md5

  def buffer = {
    val buf = ByteBuffer.allocateDirect(size)
    buf.put(bytes)
    buf.rewind()
    buf
  }

  def randomBytes(size: Int): Bytes = {
    val bytes = new Array[Byte](size)
    Random.nextBytes(bytes)
    bytes
  }

  def client = new Socket()
    .bind(new InetSocketAddress(0))
    .connect(server.getAddress)

  "Socket" should {

    "send/receive direct buffers" >> {
      actor {
        server.accept.send(buffer)
      }
      val buf = client.receiveBuffer(size)
      buf.md5 must_== md5
    }

    "send/receive wrapped buffers" >> {
      actor {
        server.accept.send(ByteBuffer.wrap(bytes))
      }

      val buf = client.receiveBuffer(size)
      buf.md5 must_== md5
    }

    "send/receive bytes" >> {
      actor {
        server.accept.send(bytes)
      }
      client.receive(size).md5 must_== md5
    }

    "send bytes receive buffer" >> {
      actor {
        server.accept.send(bytes)
      }

      val buf = client.receiveBuffer(size)
      buf.md5 must_== md5
    }

    "send buffers receive bytes" >> {
      actor {
        server.accept.send(buffer)
      }

      val buf = client.receiveBuffer(size)
      buf.md5 must_== md5
    }

    "receiving buffers of bigger size then sending" >> {
      actor {
        server.accept.send(buffer)
      }

      val buf = client.receiveBuffer(size + 10)
      buf.md5 must_== md5
    }

    "receiving buffers of smaller size then sending" >> {

      actor {
        server.accept.send(buffer)
      }

      val smaller = size - 10

      val buf = client.receiveBuffer(smaller)
      buf.md5 must_== bytes.take(smaller).md5
    }

    "receiving bytes of bigger size then sending" >> {
      actor {
        server.accept.send(bytes)
      }
      client.receive(size + 10).md5 must_== md5
    }

    "receiving bytes of smaller size then sending" >> {
      actor {
        server.accept.send(bytes)
      }

      val smaller = size - 10
      client.receive(smaller).md5 must_== bytes.take(smaller).md5
    }
  }
}