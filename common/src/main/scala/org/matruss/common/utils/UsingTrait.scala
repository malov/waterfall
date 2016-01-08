package org.matruss.common.utils

trait Closable{ def close() }

trait UsingTrait {
  def using[B]( a : Closable)(code : => B) = {
    try { code }
    finally{ a.close() }
  }

  implicit def trivialConvertor(a: Closable) = a

  def using[A,B]( a : A)(code :A => B)(implicit convertor :A =>Closable) = {
    try { code(a) }
    finally{ convertor(a).close() }
  }
}
