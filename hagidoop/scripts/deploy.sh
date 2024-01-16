i=1

while IFS=" " read -r host port; do
    echo "Deploying on $host:$port"   
    ssh $host "cd /home/gmangeno/2A/Donnees-reparties/hagidoop/scripts/;
                sh deployLocal.sh $port $i;" &
    i=$(($i+1))
    
done < /home/gmangeno/2A/Donnees-reparties/hagidoop/config/config.txt