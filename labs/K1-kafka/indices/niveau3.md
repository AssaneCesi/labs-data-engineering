# Indices du niveau 3

1. Un groupe de consommation est un jeu d'offsets. Un groupe nouveau n'a aucun offset : il part du début (réglage `auto.offset.reset`).
2. Ton consommateur accepte `--groupe`. Ne rejoue pas avec `labo-k1`.
3. `python -m lab.consommateur --groupe labo-k1-rejeu`. Pourquoi la table est-elle correcte, alors que le topic contient des doublons et du désordre ?
