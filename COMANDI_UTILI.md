# Comandi Utili

Per recuperare l'<id-container>:

    docker ps

Accedere al file system di un container in esecuzione:
        
    docker exec -it <id-container> /bin/bash
oppure
        
    docker exec -it <id-container> /bin/sh

per uscire
    
    exit