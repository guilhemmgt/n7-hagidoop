
while IFS=" " read -r host port; do
    ssh $host "pidToKill=\$(ps -x | grep \"java hdfs.HdfsServer\");
    pid=\$(echo  \$pidToKill | cut -d' ' -f 1);
    kill -9 \$pid;
    pidToKill=\$(ps -x | grep \"java daemon.WorkerImpl\");
    pid=\$(echo \$pidToKill | cut -d' ' -f 1);
    kill -9 \$pid;" &
done < /home/gmangeno/2A/Donnees-reparties-main/hagidoop/config/config.txt