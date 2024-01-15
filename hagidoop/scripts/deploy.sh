# pour chaque ligne de config
    # récupérer adresse machine et port de la ligne i
    # ssh adresse machine
        # lancer HdfsServer sur le port correpondant
        # mkdir/cd <répertoire>/node-i
    # exit ssh

i=1

while IFS=" " read -r host port; do
    echo "Deploying on $host:$port"   
    ssh $host "cd /home/gmangeno/2A/Donnees-reparties/hagidoop/scripts/;
                sh deployLocal.sh $port $i;" &
    i=$(($i+1))
    
done < /home/gmangeno/2A/Donnees-reparties/hagidoop/config/config.txt