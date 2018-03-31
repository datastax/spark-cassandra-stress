package com.datastax.sparkstress

import java.io.{FileNotFoundException, OutputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

class NoopFileSystem extends FileSystem {

  class NoopOutputStream extends OutputStream {
    override def write(b: Int): Unit = {
      // noop
    }
  }

  private var name: URI = _

  override def initialize(name: URI, conf: Configuration): Unit = this.name = name

  override def listStatus(f: Path): Array[FileStatus] = Array[FileStatus]()

  override def getFileStatus(f: Path): FileStatus = throw new FileNotFoundException()

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream =
    new FSDataOutputStream(new NoopOutputStream())

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    new FSDataOutputStream(new NoopOutputStream())

  override def getWorkingDirectory: Path = new Path("/")

  override def setWorkingDirectory(f: Path): Unit = {}

  override def mkdirs(f: Path, permission: FsPermission): Boolean = true

  override def getUri: URI = name

  override def delete(f: Path, recursive: Boolean): Boolean = true

  override def rename(src: Path, dst: Path): Boolean = true

  override def open(f: Path, bufferSize: Int): FSDataInputStream = throw new FileNotFoundException()
}
