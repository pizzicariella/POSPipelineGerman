package meta

import model.PosAnnotation
import org.scalatest.funsuite.AnyFunSuite

class ExtraInformationTest extends AnyFunSuite{

  test("getPosPercentage returns List with correct values"){
    val posList = Seq(PosAnnotation(0,3,"PRON"),PosAnnotation(5,7, "AUX"), PosAnnotation(9,11, "DET"),
      PosAnnotation(13,16,"NOUN"), PosAnnotation(18,21,"NOUN"))
    val correctResults = List(("PRON", 0.2), ("AUX", 0.2), ("DET", 0.2), ("NOUN", 0.4))
    val result = ExtraInformation.getPosPercentage(posList)
    assert(result === correctResults)
  }

}
