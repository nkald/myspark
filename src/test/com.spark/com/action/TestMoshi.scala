package com.action

object TestMoshi {

  def main(args: Array[String]): Unit = {

    val li = List(("a",1),("b",2))
    val list = List((("zhans","xie"),1),(("ls","xie"),2))
    val result = list.map{
      case ((name, xie), num) => (name, (xie, num))
    }
    println(result)

    val list01 = List("1",1,3,4,5)
    list01.collect{case a:Int => a+1}

    li.map{
      {case (x,y) => (x,y*2)}
    }
    li.map(
      f=>(f._1,f._2*2)
    )

    val user = User("ls",20)
    user match {
      case User("ls",20)=>println("匹配到了")
      case  _ =>println("没匹配到")
    }
  }
  case class User(val name:String,val age:Int) {
  }
}
