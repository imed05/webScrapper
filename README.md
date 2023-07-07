Ce repository est un projet de web scraping.

Ce scraper va extraire les balises et le contenu HTML d'un URL donné ainsi que d'autres URL à scraper à leur tour également, les données sont stockées dans une BDD MongoDB. Il peut fonctionner en mode distribué.

Ce scraper n'est pas utilisable en ligne de commande car il inclut une API Flask. Un outil type Postman peut être utilisé pour utiliser ce script.
Exemple : (http://127.0.0.1:5000/api/scrape?url=https://fr.wikipedia.org/wiki/France&first=True). 127.0.0.1:5000 est l'URL de l'application, https://fr.wikipedia.org/wiki/France l'URL à scraper, first=True permet de commencer une session, first=False permet de rejoindre une session si volonté de tourner à deux machines sur une même session.

Auteurs : Imed Eddine KEMMOUCHE, Mohammed OUHAMANE, Alexis BLAS
