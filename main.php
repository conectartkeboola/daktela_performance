<?php
// OLAP REPORTY ODVOZENÉ Z OUT-BUCKETU DAKTELA

require_once "vendor/autoload.php";

// načtení konfiguračního souboru
$ds         = DIRECTORY_SEPARATOR;
$dataDir    = getenv("KBC_DATADIR");

// pro případ importu parametrů zadaných JSON kódem v definici PHP aplikace v KBC
$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// ==============================================================================================================================================================================================
// proměnné a konstanty

// struktura tabulek
$tabsIn = [
    "loginSessions" => ["idloginsession", "start_time", "end_time", "duration", "iduser"],
    "pauseSessions" => ["idpausesession", "start_time", "end_time", "duration", "idpause", "iduser"],
    "queueSessions" => ["idqueuesession", "start_time", "end_time", "duration", "idqueue", "iduser"]/*,
    "groups"        => ["idgroup", "title"],
    "pauses"        => ["idpause", "title", "idinstance", "type", "paid"] */
];
$tabsInArr = ["loginSessions", "pauseSessions", "queueSessions"];   // vstupní tabulky, které budou převáděny na pole
$colsComm =  ["start_time", "end_time", "iduser"];                  // sloupce v poli událostí, které se vyskytují ve všech vstupních tabulkách

$tabsOut = [                                                        // názvy sloupců výstupních tabulek se prefixují v kódu níže
   "events"         => ["time", "type", "object", "iduser", "idqueue", "idpause"], 
   "performance"    => ["time", "iduser", "idgroup", "idpause", "pause_duration", "pause_duration_countable"]
];

// typy událostí
$eventTypes = ["S", "E"];                                           // S = start, E = end
 
// ==============================================================================================================================================================================================
// funkce

// ==============================================================================================================================================================================================
// načtení vstupních souborů
foreach ($tabsIn as $file) {
    ${"in_".$file} = new Keboola\Csv\CsvFile($dataDir."in".$ds."tables".$ds."in_".$file);
}
// vytvoření výstupních souborů
foreach ($tabsOut as $file) {
    ${"out_".$file} = new \Keboola\Csv\CsvFile($dataDir."out".$ds."tables".$ds."out_".$file.".csv");
}
// zápis hlaviček do výstupních souborů
foreach ($tabsOut as $tab => $cols) {
    $colsOut = array_key_exists($tab, $colsInOnly) ? array_diff(array_keys($cols), $colsInOnly[$tab]) : array_keys($cols);
    $colPrf  = strtolower($tab)."_";                    // prefix názvů sloupců ve výstupní tabulce (např. "performance" → "performance_")
    $colsOut = preg_filter("/^/", $colPrf, $colsOut);   // prefixace názvů sloupců ve výstupních tabulkách názvy tabulek kvůli rozlišení v GD (např. "time" → "performance_time")
    ${"out_".$tab} -> writeRow($colsOut);
}
// načtení vstupních tabulek sessions do pole událostí
$events = [];                                           // inicializace pole událostí
foreach ($tabsIn as $tab => $cols) {
    if (!in_array($tab, $tabsInArr)) {continue;}        // vstupní tabulky, které nebudou převáděny na pole    
    foreach ($tab as $rowNum => $row) {                 // načítání řádků tabulky [= iterace řádků]
        if ($rowNum == 0) {continue;}                   // vynechání hlavičky tabulky
        $colVals   = [];                                // řádek výstupní tabulky        
        $columnId  = 0;                                 // index sloupce (v každém řádku číslovány sloupce 0,1,2,...)
        foreach ($cols as $colName) {                   // konstrukce prvků pole (prvkem pole je vnořené pole) [= iterace sloupců]
            foreach ($eventTypes as $eventType) {       // z každého řádku vstupní tabulky vytvoří 2 řádky tabulky událostí (eventType = S / E)
                switch ($colName) {
                    case "idloginsession":  break;      // sloupec nezpracováván
                    case "idqueuesession":  break;      // sloupec nezpracováván
                    case "idpausesession":  break;      // sloupec nezpracováván
                    case "start_time":      if ($eventType == "S") {$events[]["time"] = $row[$columnId];}   break;
                    case "end_time":        if ($eventType == "E") {$events[]["time"] = $row[$columnId];}   break;
                    case "duration":        switch ($eventType) {
                                                case "S":               $events[]["type"]   = "S";  break;
                                                case "E":               $events[]["type"]   = "E";
                                            }                        
                                            switch ($tab) {
                                                case "loginSessions":   $events[]["object"] = "L";  break;
                                                case "loginSessions":   $events[]["object"] = "Q";  break;
                                                case "loginSessions":   $events[]["object"] = "P";
                                            }           // vlastní hodnota 'duration' nezpracovávána
                    case "iduser":          $events[]["iduser"]  = $row[$columnId];  break; 
                    case "idqueue":         $events[]["idqueue"] = ($tab == "queueSessions") ? $row[$columnId] : "";    break; 
                    case "idpause":         $events[]["idqueue"] = ($tab == "pauseSessions") ? $row[$columnId] : "";    break;                    
                }
            }
            $columnId++;                                // přechod na další sloupec (buňku) v rámci řádku   
        }
    }
}      

// zápis pole událostí (events) do debugovací tabulky
$out_events -> writeRow($tabsOut["events"]);            // zápis hlavičky debugovací tabulky událostí
foreach ($events as $id => $vals) {                     // $id = 0,1,2,... (nezajímavé), $ vals = 1D-pole s hodnotami řádků
    $colVals = [];                                      // inicializace řádku k zápisu
    foreach ($vals as $col => $val) {
        $colVals[]  = $val;
    }
}
?>