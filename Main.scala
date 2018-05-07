object Main {
  def main(args: Array[String]): Unit = {
    println("==== DATASET CLEANER ====")
    val filterManager = FilterManager()
    filterManager.process()
  }
}
