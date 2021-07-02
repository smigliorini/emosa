# Emosa

## Progetto tirocinio e tesi 2020/2021

### Mattia Carra VR429609

Il ruolo del contesto nelle raccomandazioni di gruppi equi: si indagherà il ruolo svolto dal contesto, ovvero la situazione che un gruppo di persone sta vivendo, nella progettazione di un sistema che raccomanda sequenze di attività come un problema di ottimizzazione multi-obiettivo, la soddisfazione del gruppo e l'intervallo di tempo a disposizione sono due delle funzioni da ottimizzare. L'evoluzione dinamica del gruppo può essere la caratteristica contestuale chiave che deve essere considerata per produrre suggerimenti equi.

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
- veronacard _ astext _ 05.csv: inserito nella directory principale ma non utile al nostro scopo

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
- ParetoSet: partendo da un insieme di soluzioni, che sono una sequenza di suggerimenti, poi per ognuno di questi verifico in modo che all'interno del paretoset ci siano solo le soluzioni che sono mutualmente non dominanti tra di loro, cioè sono buone per un aspetto ma meno per un altro, ma non posso affermare con certezza che una è migliore dell'altra. La classe è un insieme di oggetti di tipo VeronaCard, poi ho varie funzioni che trovo dentro la cartella _MosaUtil_ che viene calcolata la funzione dominanza e vengono calcolate le altre funzioni che servono per aggiornare l'insieme.

## Funzionamento

#### Hadoop

Hadoop è un framework software utilizzato per sviluppare applicazioni di elaborazione dati che vengono eseguite in un ambiente di elaborazione distribuito.

Le applicazioni create utilizzando Hadoop vengono eseguite su set di dati di grandi dimensioni distribuiti su cluster di computer comuni, così da ottenere una maggiore potenza di calcolo a basso costo.

In Hadoop, i dati risiedono in un file system distribuito chiamato *Hadoop Distributed Fyle System*. Il modello di elaborazione si basa sul concetto di *Data Locality* in cui la logica di calcolo viene inviata ai nodi del cluster (costituiti da un insieme di più unità di elaborazione (disco di archiviazione + processore) che sono collegate tra loro e agisce come un unico sistema) contenenti dati. Questa logica computazionale può essere paragonata ad una versione compilata di un programma scritto in un linguaggio di alto livello come Java nel nostro caso. Tale programma, elabora quindi i dati archiviati in Hadoop HDFS.


#### HDFS

HDFS si occupa della parte di archiviazione delle applicazioni Hadoop. Le applicazioni MapReduce utilizzano i dati da HDFS. HDFS crea più repliche di blocchi di dati e le distribuisce sui nodi di calcolo in un cluster. Questa distribuzione consente calcoli affidabili ed estremamente rapidi.

**Architettura**:
- NameNode: rappresenta tutti i file e le directory utilizzati nello spazio dei nomi

- DataNode: aiuta a gestire lo stato di un nodo HDFS e permette di interagire con i blocchi

- MasterNode: consente di condurre l'elaborazione parallela dei dati utilizzando Hadoop MapReduce

- Slave Node: sono le macchine aggiuntive nel cluster Hadoop che consentono di memorizzare i dati per condurre calcoli complessi. Inoltre, tutto il nodo slave viene fornito con Task Tracker e un DataNode. Ciò consente di sincronizzare i processi rispettivamente con NameNode e Job Tracker.

**Operazioni di lettura in HDFS**:
- Un client avvia la richiesta di lettura chiamando il metodo 'open ()' dell'oggetto FileSystem; è un oggetto di tipo DistributedFileSystem.
- Questo oggetto si connette a namenode e ottiene informazioni sui metadati come le posizioni dei blocchi del file. Notare che questi indirizzi sono i primi pochi blocchi di un file.
- In risposta a questa richiesta di metadati, vengono restituiti gli indirizzi dei DataNode che hanno una copia di quel blocco.
- Una volta ricevuti gli indirizzi dei DataNodes, al client viene restituito un oggetto di tipo FSDataInputStream. FSDataInputStream contiene DFSInputStream che si occupa delle interazioni con DataNode e NameNode. 
- I dati vengono letti sotto forma di flussi in cui il client richiama ripetutamente il metodo "read ()". Questo processo dell'operazione read () continua finché non raggiunge la fine del blocco.
- Una volta raggiunta la fine di un blocco, DFSInputStream chiude la connessione e passa alla ricerca del DataNode successivo per il blocco successivo
- Una volta che un client ha terminato la lettura, chiama un metodo close ().


**Operazioni di scrittura in  HDFS**:
- Un client avvia l'operazione di scrittura chiamando il metodo 'create ()' dell'oggetto DistributedFileSystem che crea un nuovo file.
- L'oggetto DistributedFileSystem si connette a NameNode e avvia la creazione di un nuovo file. Tuttavia, questo file crea l'operazione ma non associa alcun blocco al file. È responsabilità di NameNode verificare che il file creato non esista già e che un client disponga delle autorizzazioni corrette per creare un nuovo file. Se un file esiste già o il client non dispone di autorizzazioni sufficienti per creare un nuovo file, al client viene generata IOException. In caso contrario, l'operazione ha esito positivo e un nuovo record per il file viene creato da NameNode.
- Una volta creato un nuovo record in NameNode, al client viene restituito un oggetto di tipo FSDataOutputStream. Un client lo utilizza per scrivere dati nell'HDFS. Viene richiamato il metodo di scrittura dei dati.
- FSDataOutputStream contiene l'oggetto DFSOutputStream che si occupa della comunicazione con DataNodes e NameNode. Mentre il client continua a scrivere dati, DFSOutputStream continua a creare pacchetti con questi dati. Questi pacchetti vengono accodati in una coda chiamata DataQueue.
- C'è un altro componente chiamato DataStreamer che consuma questo DataQueue. DataStreamer chiede inoltre a NameNode l'allocazione di nuovi blocchi, selezionando così i DataNode desiderabili da utilizzare per la replica.
- Ora, il processo di replica inizia creando una pipeline utilizzando DataNodes. Un livello di replica di "n" e quindi ci saranno "n" DataNode nella pipeline.
- Il DataStreamer riversa i pacchetti nel primo DataNode nella pipeline.
- Ogni DataNode in una pipeline archivia il pacchetto ricevuto da esso e lo inoltra al secondo DataNode in una pipeline.
- Un'altra coda, "Ack Queue", viene gestita da DFSOutputStream per archiviare i pacchetti in attesa di riconoscimento da DataNodes.
- Una volta ricevuto il riconoscimento per un pacchetto nella coda da tutti i DataNode nella pipeline, viene rimosso dalla "Coda di riconoscimento". In caso di errore del DataNode, i pacchetti di questa coda vengono utilizzati per riavviare l'operazione.
- Dopo che un client ha terminato con la scrittura dei dati, chiama un metodo close (), il risultato è lo scaricamento dei pacchetti di dati rimanenti nella pipeline seguito dall'attesa del riconoscimento.
- Una volta ricevuto un riconoscimento finale, NameNode viene contattato per comunicargli che l'operazione di scrittura del file è stata completata.


#### MapReduce

MapReduce è un modello computazionale e un framework software per la scrittura di applicazioni eseguite su Hadoop. Questi programmi MapReduce sono in grado di elaborare enormi dati in parallelo su grandi cluster di nodi di calcolo, tecnica che abbiamo utilizzato anche nel nostro progetto *EMOSA (Entertainment Multi-Objective Simulated Annealing)*. 

**Architettura**:

L'input per ciascuna fase sono le coppie chiave-valore. Inoltre, ogni è necessario specificare due funzioni: funzione di mappa (*mapper*) e funzione di riduzione (*reducer*).
L'intero processo passa attraverso quattro fasi di esecuzione, ovvero divisione (*Split*), mappatura (*Mapping*), mescolamento (*Shuffling*) e riduzione (*Reducing*).

- Divisioni di input: un input per un job MapReduce in Big Data è diviso in parti di dimensioni fisse chiamate suddivisioni degli input. La suddivisione dell'input è una parte dell'input che viene consumata da una singola mappa

- Mappatura: questa è la prima fase dell'esecuzione del programma di riduzione della mappa. I dati in ogni divisione vengono passati a una funzione di mappatura per produrre valori di output. Per esempio, un compito della fase di mappatura può essere contare un numero di occorrenze di ogni parola dalle suddivisioni di input e preparare un elenco sotto forma di < parola, frequenza >, il cosiddeto HashMap in Java.

- Mescolamento: consuma l'output della fase di mappatura. Il suo compito è consolidare i record rilevanti dall'output della fase di mappatura. Per esempio, le stesse parole sono riunite insieme alla rispettiva frequenza.

- Riduzione: vengono aggregati i valori di output della fase precedente. Questa fase combina i valori e restituisce un singolo valore di output. In breve, questa fase riassume il dataset completo.

**Organizzazione Jobs**:

- Map Tasks (Split & Mapping)
- Reduce Tasks (Shuffling & Reducing)

Il processo di esecuzione completo (esecuzione dei Map & Reduce Tasks) è controllato da due tipi di entità chiamate:

- Jobtracker: agisce come un master (responsabile della completa esecuzione del lavoro inviato)
- Multipli Tasktracker: si comportano come schiavi, ognuno di loro esegue il lavoro. Per ogni lavoro inviato per l'esecuzione nel sistema, c'è un Jobtracker che risiede su Namenode e ci sono più tasktracker che risiedono su Datanode.

Un lavoro è suddiviso in più attività che vengono quindi eseguite su più nodi di dati in un cluster. È responsabilità del job tracker coordinare l'attività pianificando l'esecuzione delle attività su diversi nodi di dati. L'esecuzione delle singole attività deve quindi essere gestita dal task tracker, che risiede su ogni nodo di dati che esegue parte del lavoro. La responsabilità del task tracker è di inviare il rapporto sullo stato di avanzamento al job tracker. Inoltre, il task tracker invia periodicamente un segnale "heartbeat" a Jobtracker in modo da informarlo dello stato corrente del sistema. In questo modo il job tracker tiene traccia dell'avanzamento complessivo di ogni lavoro. In caso di fallimento dell'attività, il job tracker può riprogrammarla su un altro task tracker.


### Implementazione iniziale
- package it.univr.auditel
- importare tutte le librerie necessarie

- la classe *EstParetoFrontMapper* estende Mapper < LongWritable, Text, Text, ViewSequenceValue >
- la classe *SpatialMapper* estende Mapper < LongWritable, Text, Text, ViewSequenceWritable >
- ogni classe mapper contiene la funzione map()

- la classe *EstParetoFrontReducer* estende Reducer < Text, ViewSequenceValue, Text, ViewSequenceValue >
- la classe *SpatialReducer* estende Reducer < Text, ViewSequenceWritable, Text, ViewSequenceValue >
- ogni classe reducer contiene la funzione reduce()

- la classe *TrsaAuditel* è il main. Dopo aver ricevuto i parametri richiesti avvierà i vari job.
- args[0] = mainDirectory
- args[1] = inputDirectory
- args[2] = outputDirectory
- args[3] = ageClasses
- args[4] = timeSlot

- per creare il jar devo eseguire da linea comando: $HADOOP_HOME/bin/hadoop jar Auditel.jar /mainDirectory /inputDirectory /outputDirectory

- Funzionamento in Hadoop (e anche su IntelliJ): la cartella trsa_auditel contiene 2 sotto cartelle, input e output, la prima deve contenere sequencies history .csv, la seconda deve contenere sotto cartelle da 0 a N in base a quale istruzione viene scelta che poi produrrà file di output; gli altri file csv rimangono nella cartella main.

### Lista canali
- generate tutte le combinazioni ed assegnare un double che indica la probabilità di switching da un canale all'altro (channel_transition.csv)
- canali : da 1 a 121 inclusi (tranne il 24), quindi in totale ci sono 120 canali e non 119 come scritto nel paper)

#### TODO - tesi
- Presentazione finale con orale (15 minuti circa)
- Spiegazione generale con funzionamento architettura
- Approfondimento tematica principale
- Latex document personale

#### TODO - progetto
- Modificare le classi contenenti i metodi MapReduce (setup), passando la transitionMap.
- Nella classe ViewSequenceValue, nel metodo viene calcolata la preferenza. Dobbiamo introdurre anche una preferenza che dipende dalla transizione dei canali.
- Per calcolare il ViewSequenceValue nel costruttore, oltre a passare la sequenza, la preferenceMap e lo schedulingMap, devo aggiungere anche la transitionMap.
- Poi devo cambiare il metodo writeReadField() e il toString(), sulla falsa riga di quello già fatto per le userPreferences.
- Altra cosa il metodo dominate(), dove invece esserci solo 4 valori, dovrò metterne 5 in cui andrò a confrontare anche (avrò un metodo da fare chiamato compareTransition)
- La mia proprietà potrebbe essere una lista di Double in cui ad ogni elemento ci assegno la preferenza di fare la transizione da un canale al successivo; quindi ho la channelSequence dentro la classe contenente la sequenza di canali che va a guardare l'utente, l'altra proprietà è una lista di Double che sono le transitionPreferences, dove l'elemento 0 diventa la preferenza associata alla transizione dela canale da 0 a 1, il secondo elemento è la preferenza nel fare la transizione da 1 a 2 e così via. Quindi nel costruttore ricevo la mappa che ho costruito prima, ma che poi mi servirà per inizializzare questa lista di Double che mi daranno la preferenza di transizione fra ogni coppia, quindi nella posizione i ci metto la preferenza di fare la transizione da i a i+1, in modo tale che quando andrò a calcolare la dominate(), nel fare il compare channelPreference dovrò fare: sommare tutte i Double e trovare un numero unico, a questo punto se il numero di *other* ottenuto è più piccolo del numero di *this*, quella dominate() tornerà true, altrimenti false (gli Objective value vengono confrontati tutti). Quindi andrò a calcolare un objective value 4, cioè nella quinta posizione, che fa questo compare(), che valuta se *this* > *other* per quella componente e ritorna un int, come il classico compareTo() di Java (0 se sono uguali, 1 se il primo > del secondo, -1 se è minore). 
- Dopo aver fatto queste modifiche opererò su un altro punto del programma, il ViewSequenceWritable, che prende una lista di visioni e lo adatterò successivamente.
- Riassumendo: aggiungiamo questa nuova funzione obiettivo, poi la devo far lavorare nelle perturbazioni, cioè data una sequenza devo operare dei microcambiamenti, quindi cambiare un programma in mezzo, valutando quindi se la nuova soluzione è migliore di quella precedente, tenendo quindi conto della nuova preferenza di transizione dei canali dentro la sequenza, quindi invece di tenere un valore unico, ci interessa la somma alla fine, tengo tutta la lista, perchè quando vado a perturbare (cambio un elemento all'interno della sequenza) mi basterà cambiare un solo elemento anche nella sequenza delle preferenze di transizione.

## NEW TODO CODICE
- Nel costruttore di ViewSequenceValue inizializzo le transitionPreferences a vuoto perchè poi nel setup del map che carica il file andrà a popolarla come è stato fatto per le userPreferences
- In MosaUtil nel metodo dominate devo aggiungere la parte nel return delle transitionPreferences
- Manca la parte di analisi risultati --> confronto dei dati tra prima e dopo le nuove modifiche

## NEW TODO PRESENTAZIONE
- Slide 12 minuti max, max 10 slide e mostrare esecuzione del programma nel seguente modo: eseguo con i dati che ho e calcolo dal mio valore risultante le varie funzioni obiettivo (come nel file results.csv), manca nel calcolo delle %RSD della preferenza sul cambio del canale, quindi estendo la funzione che mi fornisce il risultato così da ottenere anche questo valore, sempre sulla falsa riga delle userPreferences. Quindi lancio il programma coi file che ho generato nuovi e anche con un altro file in cui la preferenza di transizione dal canale A al B è sempre uguale, quindi è come se non la tenessi in considerazione e valuto cosa cambia nei risultati, cioè la %RSD non la valuto nemmeno.
- Devo quindi modificare la classe che mi fornisce i risultati in output
- Nella presentazione faccio una intro che spiega in generale il problema che ho affrontato, piccola parte su Hadoop, HDFS e MapReduce, e poi spiego il mio contributo dove parlo cosa ho aggiunto, cioè la parte delle preferenze sulla transizione tra canali proprio perchè si è visto che dando una raccomandazione che è una sequenza di raccomandazioni, è opportuno mantenere un'uniformità di genere di suggerimenti, senza fare passaggi che non piacciano agli utenti, anche se, per esempio il canale A e B piacciono allo stesso modo all'utente, si è notato che dati in sequenza, uno preferisce non passare da uno all'altro (questione di Fairness e mood, cioè uno stato d'animo costante, che non varia troppo durante le transizioni)
- Nel main TrsaAuditel, ho il nuovo parametro transitionValue passato all'avvio del programma. Invece di creare un doppio file che ho la preferenza di transizione tra canali a 0, posso valutare che se all'avvio non trovo il file transitionPreferences.csv, allora posso dire che qualsiasi transizione vale 0 (caso di default), questo controllo lo devo fare nel setup del Map e Reduce dove, quando tenta di caricare il file per inizializzare (parte readChannelTransition) vado ad aggiungere questo controllo --> if(transitionMap == 0) allora la transitionMap non la inizializzo e rimane vuota, ovviamente se metto null poi non posso usare la .get() per un certo valore, se invece la metto a vuota allora il singolo get() mi ritorna null e posso mettere 0





































