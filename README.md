# JMR Word Count Client

Questo repository contiene due fat jar eseguibili per JMR:

- `jmr-wc.jar`: job di word count che salva opzionalmente il risultato in CSV.
- `jmr-wc-failing.jar`: job di test che forza un errore in fase `map` o `reduce`.

## Submit a un cluster

### Word count

```bash
java -jar jmr-wc.jar <serialized-books-data-path> jmr-wc.jar <master-host> <master-port> [csv-output-path]
```

Esempio:

```bash
java -jar jmr-wc.jar data/serialized jmr-wc.jar 192.168.1.50 9999 outputs/result.csv
```

### Failing job

```bash
java -jar jmr-wc-failing.jar <serialized-books-data-path> jmr-wc-failing.jar <master-host> <master-port> <map|reduce>
```

Esempio:

```bash
java -jar jmr-wc-failing.jar data/serialized jmr-wc-failing.jar 192.168.1.50 9999 map
```

Il secondo argomento `jar-path` deve puntare allo stesso jar si sta eseguendo, esso contiene il codice che verrà eseguito

## Immagini docker dei vari componenti

- [leo02n/jmr-wc](https://hub.docker.com/r/leo02n/jmr-wc)
- [leo02n/jmr-wc-failing](https://hub.docker.com/r/leo02n/jmr-wc-failing)
