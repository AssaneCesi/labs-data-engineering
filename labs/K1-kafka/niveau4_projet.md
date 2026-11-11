# Niveau 4 : appliquer au projet plateforme-data

Tu sais ce qu'est un topic, une clé, un offset, un groupe, et pourquoi un consommateur doit être idempotent. Applique-le à ton pipeline : **sur le papier d'abord**, puis par un petit prototype.

## Tâches

1. **Concevoir.** Dans `docs/kafka_sirene.md`, décris comment les mises à jour SIRENE quotidiennes circuleraient dans Kafka : quels topics, quelle **clé** (siret ? siren ?), combien de partitions et pourquoi, quelle rétention, quel format de message (la version vient d'où : `dateDernierTraitementEtablissement` ?).
2. **Réutiliser.** Montre quelle fonction de `plateforme-data` jouerait le rôle de `appliquer_message` (ton chargement incrémental idempotent de E2) et ce qu'il faudrait y changer.
3. **Prototyper.** Rejoue un extrait de l'échantillon (quelques milliers de lignes) dans un topic local et applique-le dans `data/entrepot_echantillon.duckdb`, avec un consommateur qui valide ses offsets après l'écriture.
4. **Questions.** Réponds par écrit : qu'est-ce qui changerait entre ce flux et ton chargement en lots actuel (latence, reprise sur incident, ordre) ? Quand Kafka est-il un bon choix pour ce pipeline, et quand est-il de la sur-ingénierie ?

## Critères d'acceptation (visibles dans le dépôt)

- `docs/kafka_sirene.md` existe, avec le choix de la clé justifié.
- Le prototype rejouable deux fois donne le même état (comptages avant et après dans `docs/journal.md`).
- Tu réponds en deux minutes à : « Comment garantis-tu qu'un consommateur Kafka ne crée pas de doublons ? » et « Pourquoi pas Kafka pour 3,7 millions de lignes par jour en lot ? ».

Coche ensuite Kafka dans Cap Data Quality (sujet hors codes : à ajouter à ta liste).
