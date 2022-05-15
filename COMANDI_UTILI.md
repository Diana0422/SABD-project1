# Comandi Utili

Per recuperare l'<id-container> e il nome:

    docker ps --format "table {{.ID}}\t{{.Names}}"

Accedere al file system di un container in esecuzione:
        
    docker exec -it <id-container> /bin/bash
oppure
        
    docker exec -it <id-container> /bin/sh

per uscire
    
    exit