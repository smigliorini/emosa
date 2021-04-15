# Emosa

## Progetto tirocinio e tesi 2020/2021

### Mattia Carra VR429609

Il ruolo del contesto nelle raccomandazioni di gruppi equi: si indagherà il ruolo svolto dal contesto, ovvero la situazione che un gruppo di persone sta vivendo, nella progettazione di un sistema che raccomanda sequenze di attività come un problema di ottimizzazione multi-obiettivo, la soddisfazione del gruppo e l'intervallo di tempo a disposizione sono due delle funzioni da ottimizzare. L'evoluzione dinamica del gruppo può essere la caratteristica contestuale chiave che deve essere considerata per produrre suggerimenti equi.

#### Configurazione
Importare il progetto da IntelliJ o in un altro IDE compatibile con Java
Installare Git per una migliore gestione della repository online

#### Test applicazione

##### Prerequisites 
- Java installato
- JAVA_HOME configurato
- Hadoop installato 
- HADOOP_HOME configurato
- JAR file esportato in HADOOP_CLASSPATH

#### Comandi
Dopo aver fatto partire i componenti essenziali di Hadoop, l'HDFS con start-dfs.sh e Yarn con start-yarn.sh, è stato necessario ai fini di testing creare una directory trsa_auditel (con hadoop fs -mkdir) in Hadoop.
Al suo interno, si trovano altre 2 directory, input e output.

Input contiene il dataset, in particolare si notano 5 file .csv:
- epg_ program _scheduling.csv: contiene la lista di programmi disponibili con il loro genere e l'orario di messa in onda.
- group_ type _evolution.csv: contiene le possibili evoluzioni di un gruppo dato il suo tipo.
- user_ channel _ timeslot _ wdwe _ seconds _preferences.csv: contiene le preferenze dei singoli utenti per time slot e giorno della settimana.
- sequences_history.csv: è l'input vero e proprio, contiene le sequenze passate su cui eseguire l'algoritmo.
- veronacard_ astext _05.csv: contiene altre informazioni

Output contiene i risultati, sempre in formato .csv, in particolare viene creata una cartella da 0 a n in base a quante volte il programma viene invocato.

Alla fine delle operazioni ricordarsi di fermare i processi e i job attivi con stop-yarn.sh e stop-dfs.sh.

#### Altri comandi
Particolarmente utili sono stati i comandi per trasferimento file dalla repository locale al fyle system di Hadoop, (_hadoop fs -copyFromLocal source destination_), per listare i file presenti in HDFS, (_hadoop fs -ls nomeDir_), e per portare i risultati dal fyle system di Hadoop in locale, (_hadoop fs -copyToLocal source destination_).
Dopo le opportune modifiche bisogna esportare il jar ottenuto dentro Hadoop prima di lanciare il programma in HDFS.
E' possibile vedere il contenuto dell'output da linea di comando con l'opzione -cat oppure accedere al fyle system direttamente dal browser tramite _localhost:50070_.

### Obiettivo
Per far funzionare la nuova versione del programma sarà necessario modificare in particolare  user_ channel _ timeslot _ wdwe _seconds_preferences.csv  in modo che contenga preferenze dinamiche che cambiano rispetto ai vari criteri citati, non solo il tempo.


