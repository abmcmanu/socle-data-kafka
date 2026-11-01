# Schémas

Les schémas sont dessinés avec [Excalidraw](https://excalidraw.com). Chaque fichier `.excalidraw.svg`
contient à la fois l'image affichée dans la documentation et la scène éditable.

Pour modifier un schéma : ouvrir [excalidraw.com](https://excalidraw.com), menu *Ouvrir*, choisir le
fichier `.excalidraw.svg`, modifier, puis *Exporter l'image* → SVG en cochant *Intégrer la scène*, et
remplacer le fichier sous le même nom.

| Fichier | Utilisé dans |
| --- | --- |
| `architecture-socle` | [README](../../README.md) |
| `vue-contexte`, `vue-deploiement` | [Architecture cible](../architecture/01-architecture-cible.md) |
| `flux-commande`, `flux-cdc` | [Flux de données](../architecture/02-flux-de-donnees.md) |
| `securite-couches` | [Sécurité](../architecture/04-securite.md) |
| `ordre-arret-relance` | [Arrêt et relance](../exploitation/02-procedures-arret-relance.md) |
| `chaine-supervision` | [Supervision](../exploitation/03-supervision.md) |
| `pra-sites` | [PRA](../exploitation/05-plan-reprise-activite.md) |
| `depot-hadoop` | [Intégration Hadoop](../../hadoop/README.md) |
| `topologie-order-enrichment` | [Services Java](../../services/README.md) |
