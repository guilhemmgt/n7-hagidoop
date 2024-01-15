# pour chaque ligne de config
    # récupérer adresse machine et port de la ligne i
    # ssh adresse machine
        # lancer HdfsServer sur le port correpondant
        # mkdir/cd <répertoire>/node-i
    # exit ssh

i=1
        
while IFS=" " read -r host port; do
    ssh $host "cd /home/gmangeno/2A/Donnees-reparties-main/hagidoop/src/;
                mkdir -p /home/gmangeno/2A/Donnees-reparties-main/node-$i/;
                nohup java hdfs.HdfsServer $port /home/gmangeno/2A/Donnees-reparties-main/node-$i/;
                nohup java daemon.WorkerImpl $(($port+1)) /home/gmangeno/2A/Donnees-reparties-main/node-$i/;" &
    i=$(($i+1))
    
done < /home/gmangeno/2A/Donnees-reparties-main/hagidoop/config/config.txt