package json

import json.JsonParser.parseDocumentText
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class JsonParserTest extends AnyFunSuite{

  val pathToTestFile = "src/test/resources/testArticle.json"
  val json = readTestFile(pathToTestFile)
  val columns = Array("text", "intro", "title")
  val title = "Delhi - Kurzschluss wohl Ursache für Brand mit 43 Toten"
  val intro = "Bei einem Großbrand in einer Fabrik in der indischen Hauptstadt Neu-Delhi sind mindestens 43 Menschen gestorben.Ursache für das Unglück in der illegal betriebenen Fabrik war wohl ein Kurzschluss.Der Besitzer wurde festgenommen, gegen ihn wird wegen fahrlässiger Tötung ermittelt."
  val text = "Ein Kurzschluss war offenbar verantwortlich für einen der schlimmsten Brände in Indiens Hauptstadt Neu-Delhi, bei dem am frühen Sonntagmorgen mindestens 43 Menschen starben. Es traf vor allem Wanderarbeiter aus dem armen östlichen Bundesstaat Bihar, die auf der Suche nach einem Job nach Delhi gekommen waren und in einer illegal betriebenen Fabrik schliefen, als das Feuer zwischen vier und fünf Uhr morgens ausbrach.\n\nGegen den Besitzer des Gebäudes in der Altstadt von Delhi wird nun wegen fahrlässiger Tötung ermittelt. Wie die Zeitung Times of India berichtete, war der Mann zunächst geflohen, konnte aber am späten Nachmittag festgenommen werden.\n\nEin erster Alarm war bei der Feuerwehr gegen 5.20 Uhr eingegangen, doch in den engen Gassen ist der Zugang für die Löschfahrzeuge schwierig. Die Rettungsarbeiten waren auch deshalb mühsam, weil die Feuerwehrleute erst Gitter vor den Fenstern beseitigen mussten, um überhaupt ins Innere des Gebäudes zu gelangen und den Brand zu löschen. Dennoch konnten die Rettungsmannschaften 63 Überlebende bergen, sie wurden mit Brandwunden und Rauchvergiftungen in Krankenhäuser eingeliefert.\n\nAuch Helfer wurden bei dem komplizierten Einsatz verletzt. Bilder zeigten Delhis Innenminister, wie er den ersten Feuerwehrmann im Gebäude, Rajesh Shukla, als \"tapferen Helden\" feierte, er alleine rettete 11 Menschen aus dem Feuer, bevor er sich die Beine verletzte. Die Feuerwehrleute mussten jeden Arbeiter einzeln auf den Schultern aus dem vierstöckigen Gebäude ins Freie schleppen. \"Es war sehr dunkel da drinnen\", sagte Einsatzleiter Sunil Choudhary, was die Arbeit für die Helfer noch ein Stück gefährlicher und mühsamer machte.\n\nDie meisten Opfer starben am Rauch, wie der Chefarzt eines Krankenhauses mitteilte. Im Inneren der Fabrik lagerte offenbar auch sehr viel Kunststoff, was die Entwicklung giftigen Qualms noch erheblich verstärkte.\n\nStädte auf dem indischen Subkontinent sind sehr anfällig für Großfeuer, weil Bauvorschriften oft nicht eingehalten werden, die Elektrik fahrlässig verlegt wurde und mangelhafte Kontrollen die Brandrisiken erhöhen. Auch dem ausgebrannten Gebäude in Delhi fehlten die nötigen Brandschutzpapiere.\n\nÄrger über einen Mangel an staatlicher Kontrolle\n\nDas Unternehmen produzierte vor allem Papier, aber auch Hand- und Schultaschen, für den lokalen Markt in Indien. Viele kleinere Fabriken und Werkstätten innerhalb der Hauptstadt werden illegal betrieben, ihre Besitzer bestechen die Behörden, um in Hinterhöfen, außerhalb der ausgewiesenen Industriegebiete, Gebäude für ihre Produktion zu nutzen. Die Arbeiter in der verbrannten Fabrik sollen kaum mehr als zwei Dollar am Tag in langen Schichten verdient haben, hieß es in Medienberichten aus Delhi, nicht einmal zum Schlafen verließen sie ihre Arbeitsplätze.\n\nIn den sozialen Medien entlud sich Ärger über einen Mangel an staatlicher Aufsicht, Kritiker beklagten Fahrlässigkeit und die grassierende Korruption, die solche Unfälle erst möglich machten. Delhis Ministerpräsident Arvind Kejriwal versprach Entschädigungen für die Familien der Opfer, seine Regierung gerät nun wegen des verheerenden Feuers in der Innenstadt unter Druck. Nur einmal waren in der indischen Hauptstadt bei einem Feuer noch mehr Menschen ums Leben gekommen: Im Jahr 1997 starben in einem Kino 59 Menschen, die meisten Besucher erstickten, nachdem ein Transformator explodiert war.\n\nEine der schlimmsten Brandkatastrophen, die Indien jemals erlebte, ereignete sich im Jahr 1995 in einer Schule im Bundesstaat Haryana. Auch damals war ein Kurzschluss die Ursache des Feuers. Der Brand löste dann auch noch eine verheerende Massenpanik aus. Insgesamt 442 Menschen kamen an jenem 23. Dezember ums Leben."
  val long_url = "https://www.sueddeutsche.de/panorama/fabrikbrand-delhi-indien-1.4714938"
  val parsedJson = parseDocumentText(json, columns)

  test("parseDocumentText should parse specified columns"){
    assert(parsedJson.contains(title))
    assert(parsedJson.contains(intro))
    assert(parsedJson.contains(text))
  }

  test("parseDocumentText should not parse other columns"){
    assert(!parsedJson.contains(long_url))
  }

  def readTestFile(path: String): String = {
    val bufferedSource = Source.fromFile(path)
    val json = bufferedSource.getLines().next()
    bufferedSource.close()
    json
  }

}
