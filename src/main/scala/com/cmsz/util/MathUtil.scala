package com.cmsz.util

object MathUtil {
  def divide(x:Double,y:Double): Double ={
    if(y==0) 0 else (x/y).formatted("%.4f").toDouble
  }
}
