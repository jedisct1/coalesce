
import java.net.URI
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.log4j.{BasicConfigurator, Level, Logger}

class Coalescer(conf: Configuration) {
  protected val logger = Logger.getLogger(getClass.getName)

  protected def getDirectories(path: Path) = path.getFileSystem(conf).
    listStatus(path).filter(_.isDirectory).map(_.getPath)

  protected def tmpPath(path: Path) = path.suffix(".tmp")

  def coalesce(path: Path) {
    logger.info(s"Coalescing [$path]")
    val fs = path.getFileSystem(conf)
    val pathMerged = tmpPath(path)
    fs.delete(pathMerged, false)
    FileUtil.copyMerge(fs, path, fs, pathMerged, true, conf, null)
    fs.rename(pathMerged, path)
  }

  def coalesceLeaves(rootPath: Path, depth: Int, minDepth: Int) {
    val paths = getDirectories(rootPath)
    if (paths.length == 0) {
      if (depth > minDepth) {
        coalesce(rootPath)
      }
    } else {
      paths.foreach(coalesceLeaves(_, depth + 1, minDepth))
    }
  }

  def coalesceLeaves(rootPath: Path, minDepth: Int): Unit =
    coalesceLeaves(rootPath, 0, minDepth)
}

object Coalesce {
  def usage() {
    println("Usage: Coalesce <min depth> <root path>")
    System.exit(1)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      usage();
    }
    BasicConfigurator.configure
    Logger.getRootLogger().setLevel(Level.INFO);
    val conf = new Configuration(true)
    val (minDepth, rootPath) = (args(0).toInt, new Path(args(1)))
    val coalescer = new Coalescer(conf)
    coalescer.coalesceLeaves(rootPath, 1)
  }
}
