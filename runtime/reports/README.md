Les rapports du matin, à raison de un fichier par journée couverte.

Produit par _dag_daily_.

Le chemin est déclaré dans _governance.yml_, sous _report.root_, et monté dans le conteneur par _docker-compose.yml_.
Les deux actions sont necessaires : un chemin déclaré sans montage donne :
```
PermissionError: /opt/project/reports
```

`/opt/project` étant un répertoire de l'image et non un volume.
