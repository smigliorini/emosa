# Emosa

## Progetto tirocinio e tesi 2020/2021

### Mattia Carra VR429609

Il ruolo del contesto nelle raccomandazioni di gruppi equi: si indagherà il ruolo svolto dal contesto, ovvero la situazione che un gruppo di persone sta vivendo, nella progettazione di un sistema che raccomanda sequenze di attività come un problema di ottimizzazione multi-obiettivo, la soddisfazione del gruppo e l'intervallo di tempo a disposizione sono due delle funzioni da ottimizzare. </b> L'evoluzione dinamica del gruppo può essere la caratteristica contestuale chiave che deve essere considerata per produrre suggerimenti equi.

#### Configurazione
Importare il progetto da IntelliJ o in un altro IDE compatibile con Java.

Installare Git per una migliore gestione della repository online.

#### Test applicazione

##### Prerequisiti 
- Java installato
- JAVA_HOME configurato
- Hadoop installato 
- HADOOP_HOME configurato
- JAR file esportato in HADOOP_CLASSPATH

#### Comandi
Dopo aver fatto partire i componenti essenziali di Hadoop, l'HDFS con start-dfs.sh e Yarn con start-yarn.sh, è stato necessario ai fini di testing creare una directory trsa_auditel (con hadoop fs -mkdir) in Hadoop.
Al suo interno, si trovano altre 2 directory, input e output.

Input contiene il dataset, in particolare si notano 5 file .csv:
- epg _ program _ scheduling.csv: contiene la lista di programmi disponibili con il loro genere e l'orario di messa in onda.
- group _ type _ evolution.csv: contiene le possibili evoluzioni di un gruppo dato il suo tipo.
- user _ channel _ timeslot _ wdwe _ seconds _ preferences.csv: contiene le preferenze dei singoli utenti per time slot e giorno della settimana.
- sequences _ history.csv: è l'input vero e proprio, contiene le sequenze passate su cui eseguire l'algoritmo.
- veronacard _ astext _ 05.csv: contiene altre informazioni (specificare poi quali)

Output contiene i risultati, sempre in formato .csv, in particolare viene creata una cartella da 0 a n in base a quante volte il programma viene invocato.

Alla fine delle operazioni ricordarsi di fermare i processi e i job attivi con stop-yarn.sh e stop-dfs.sh.

#### Altri comandi
Particolarmente utili sono stati i comandi per trasferimento file dalla repository locale al fyle system di Hadoop, (_hadoop fs -copyFromLocal source destination_), per listare i file presenti in HDFS, (_hadoop fs -ls nomeDir_), e per portare i risultati dal fyle system di Hadoop in locale, (_hadoop fs -copyToLocal source destination_).
Dopo le opportune modifiche bisogna esportare il jar ottenuto dentro Hadoop prima di lanciare il programma in HDFS.
E' possibile vedere il contenuto dell'output da linea di comando con l'opzione -cat oppure accedere al fyle system direttamente dal browser tramite _localhost:50070_.

### Obiettivo
- Per far funzionare la nuova versione del programma sarà necessario modificare in particolare  user _ channel _ timeslot _ wdwe _ seconds _ preferences.csv  in modo che contenga preferenze dinamiche che cambiano rispetto ai vari criteri citati, non solo il tempo.
- In particolare vogliamo aggiungere altre caratteristiche, che sono: creare un altro file in cui per ogni coppia di canali, quindi per ogni utente e per ogni contesto che per noi è dato dal timeslot e dal giorno della settimana devo mettere una coppia di canali e una preferenza, cioè la preferenza di guardare 2 canali consecutivi. Devo quindi creare un file csv che prende per ogni utente e per ogni canale che trovo nel file csv di esempio, faccio permutazioni dei canali e metto una preferenza con numero reale da 0 a 1, quindi poi dovrò modificare il codice sorgente in modo che venga letto il nuovo file, nello stesso modo in cui vengono letti gli altri file csv e lo carico in memoria, poi posso usare questa preferenza nel calcolo per il suggerimento complessivo. Riassumendo: prevedo un file di questo tipo, lo genero random basandomi sui valori degli utenti e dei canali, quindi devo prendere per ogni utente, coppie di canali, timeslot, giorno della settimana, e ci assegno un numero reale da 0 a 1. Provare inizialmente se funziona poi valutare se testare il tutto con un file simile proveniente dal DB. Guardo quindi come sono stati presi gli altri file dal programma e aggiungo quella parte che legge e fa il parsing del file e lo carico in memoria, proprio come fa il programma con gli altri file.
- Hint: parto dal main TrsaAuditel, guardo come vengono processati quei file li e ne creo uno simile per dare la preferenza di transizione da un canale all'altro

#### Domande e risposte
- ParetoSet: partendo da un inieme di soluzioni, che sono una sequenza di suggerimenti, poi per ognuno di questi verifico in modo che all'interno del paretoset ci siano solo le soluzioni che sono mutualmente non dominanti tra di loro, cioè sono buone per un aspetto ma meno per un altro, ma non posso affermare con certezza che una è migliore dell'altra. La classe è un insieme di oggetti di tipo VeronaCard, poi ho varie funzioni che trovo dentro la cartella _MosaUtil_ che viene calcolata la funzione dominanza e vengono calcolate le altre funzioni che servono per aggiornare l'insieme.
- Esecuzione programma su cluster Hadoop dovrebbe essere corretta anche se abbastanza lenta (chiedere perchè)
- Esecuzione tramite IntelliJ dà un errore e va fixato




