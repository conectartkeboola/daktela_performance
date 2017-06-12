<?php
// PERFORMANCE REPORT ODVOZENÝ Z OUT-BUCKETU DAKTELA

require_once "vendor/autoload.php";

// načtení konfiguračního souboru
$ds         = DIRECTORY_SEPARATOR;
$dataDir    = getenv("KBC_DATADIR");

// pro případ importu parametrů zadaných JSON kódem v definici PHP aplikace v KBC
$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// časový rozsah historie pro tvorbu reportu
$reportIntervHistDays = $config['parameters']['reportIntervHistDays'];              // pole má klíče "start" a "end", kde musí být "start" >= "end"

// ==============================================================================================================================================================================================

$tabsIn = ["groups", "queues", "queueSessions", "pauseSessions", "activities", "records"];    // vstupní tabulky

$tabsOut = [                                                                        // výstupní tabulky
    "users"     =>  ["date", "idgroup", "iduser", "Q", "QA", "QAP", "QP", "P", "AP", "queueSession", "pauseSession",
                     "talkTime", "idleTime", "activityTime", "callCount", "callCountAnswered", /*"transactionCount",*/
                     "recordsTouched", "recordsDropped", "recordsTimeout", "recordsBusy", "recordsDenied"],
    "events"    =>  ["idgroup", "iduser", "time", "type", "method"]
];         
$tabsOutList = array_keys($tabsOut);
// ==============================================================================================================================================================================================
// časový rozsah reportu

$actualDatestamp = strtotime(date('Y-m-d'));
$reportIntervDates    = [   "start" =>  date('Y-m-d',(strtotime(-$reportIntervHistDays["start"].' day', $actualDatestamp))), 
                            "end"   =>  date('Y-m-d',(strtotime(-$reportIntervHistDays["end"]  .' day', $actualDatestamp)))
                        ];
$reportIntervTimes    = [   "start" =>  $reportIntervDates["start"].' 00:00:00', 
                            "end"   =>  $reportIntervDates["end"]  .' 23:59:59'
                        ];
// ==============================================================================================================================================================================================
// načtení vstupních souborů

foreach ($tabsIn as $file) {
    ${$file} = new Keboola\Csv\CsvFile($dataDir."in".$ds."tables".$ds.$file.".csv");
}
// vytvoření výstupních souborů
foreach ($tabsOutList as $file) {
    ${"out_".$file} = new \Keboola\Csv\CsvFile($dataDir."out".$ds."tables".$ds."out_".$file.".csv");
}
// zápis hlaviček do výstupních souborů
foreach ($tabsOut as $tab => $cols) {
    $colPrf  = "report_performance_".strtolower($tab)."_";  // prefix názvů sloupců ve výstupní tabulce (např. "loginSessions" → "loginsessions_")
    $cols    = preg_filter("/^/", $colPrf, $cols);          // prefixace názvů sloupců ve výstupních tabulkách názvy tabulek kvůli rozlišení v GD (např. "title" → "groups_title")
    ${"out_".$tab} -> writeRow($cols);
}
// ==============================================================================================================================================================================================
// funkce

function initUsersAndEventsItems ($date, $iduser, $idgroup) {
    global $users, $events;    
    // inicializace záznamů do pole uživatelů
    if (!array_key_exists($date, $users)) {
        $users[$date] = [];
    }
    if (!array_key_exists($iduser, $users[$date])) {
        $users[$date][$iduser] = [];
    }
    if (/*!empty($idgroup) &&*/ !array_key_exists($idgroup, $users[$date][$iduser])) {
        $user = [                                   // sestavení záznamu do pole uživatelů
            "iduser"            => $iduser,
            "Q"                 => NULL,
            "QA"                => NULL,
            "QAP"               => NULL,
            "QP"                => NULL,
            "P"                 => NULL,
            "AP"                => NULL,
            "queueSession"      => NULL,
            "pauseSession"      => NULL,           // pauseSessions nezávisí na skupinách -> počítají se v prázdné skupině
            "talkTime"          => NULL,
            "idleTime"          => NULL,                            
            "activityTime"      => NULL,
            "callCount"         => NULL,
            "callCountAnswered" => NULL,
            //"transactionCount"  => 0,
            "recordsTouched"    => NULL,
            "recordsDropped"    => NULL,
            "recordsTimeout"    => NULL,
            "recordsBusy"       => NULL,
            "recordsDenied"     => NULL
        ];
        $users[$date][$iduser][$idgroup] = $user;
    }   
    // inicializace záznamů do pole událostí
    if (!array_key_exists($date, $events)) {
        $events[$date] = [];
    }
    if (!array_key_exists($iduser, $events[$date])) {
        $events[$date][$iduser] = [];
    }
    if (!array_key_exists($idgroup, $events[$date][$iduser])) {
        $events[$date][$iduser][$idgroup] = [];
    }
}
function dateIncrem ($datum, $days = 1) {       // inkrement data o $days dní
    return date('Y-m-d',(strtotime($days.' day', strtotime($datum))));
}
function findInArray ($key, $arr) {
    return array_key_exists($key, $arr) ? $arr[$key] : "";
} 
function addEventPairToArr ($startTime, $endTime, $type) {  // zápis páru událostí (začátek - konec) do pole událostí
    global $events, $processedDate, $iduser, $idgroup, $users, $typeAct, $itemJson, $item;
    initUsersAndEventsItems ($processedDate, $iduser, $idgroup);
    switch ($type) {
        case "Q":   $user[$processedDate][$iduser][$idgroup]["queueSession"] += strtotime($endTime) - strtotime($startTime);    break;
        case "P":   $user[$processedDate][$iduser][$idgroup]["pauseSession"] += strtotime($endTime) - strtotime($startTime);    break;
        case "A":   if ($typeAct == 'CALL' && !empty($itemJson)) {
                        $users[$processedDate][$iduser][$idgroup]["activityTime"] += strtotime($endTime) - strtotime($startTime);
                        $users[$processedDate][$iduser][$idgroup]["talkTime"]     += $item-> duration;      // parsuji duration z objektu $item
                        $users[$processedDate][$iduser][$idgroup]["callCount"]    += 1;
                        if ($item-> answered == "true") {                                                   // parsuji answered z objektu $item
                            $users[$processed_date][$a_iduser][$a_idgroup]["callCountAnswered"] += 1;
                        }
                    }
    }
    $event1 = [ "time"      =>  $startTime,
                "type"      =>  $type,
                "method"    =>  "+"             
    ];
    $event2 = [ "time"      =>  $endTime,
                "type"      =>  $type,
                "method"    =>  "-"               
    ];
    $events[$processedDate][$iduser][$idgroup][] = $event1; 
    $events[$processedDate][$iduser][$idgroup][] = $event2;
}
function sessionsProcessing ($startTested, $endTested, $type) {         // čas začátku a konce (+ typ) testované session 
    global $events, $processedDate, $iduser, $idgroup;
    $startSaved = $endSaved = NULL;                                     // čas začátku a konce porovnávané uložené session
    if (array_key_exists($processedDate, $events)) {
        if (array_key_exists($iduser, $events[$processedDate])) {
            if (array_key_exists($idgroup, $events[$processedDate][$iduser])) {
                foreach ($events[$processedDate][$iduser][$idgroup] as $event) {
                    if ($event["type"]==$type && $event["method"]=="+") {$startSaved = $event["time"];}
                    if ($event["type"]==$type && $event["method"]=="-" && !is_null($startSaved)) {$endSaved = $event["time"];}
                    if (!is_null($startSaved) && !is_null($endSaved)) {                            
                        // případ 1 - testovaná session leží celá v dřívějším nebo pozdějším čase než porovnávaná uložená session
                        if (($startTested <  $startSaved && $endTested <= $startSaved) ||
                            ($startTested >= $endSaved   && $endTested >  $endSaved) ) {
                            //sessionsProcessing ($startTested, $endTested, $type);   // rekurzivní test zbylého intervalu
                            addEventPairToArr ($startTested, $endTested, $type);
                            return; 
                        }
                        // případ 2 - testovaná session leží celá uvnitř porovnávané uložené session
                        if ($startTested >= $startSaved && $startTested < $endSaved && $endTested <= $endSaved) {
                            return;                                                 // testovaná sessiun už je celá v poli $sessions
                        }
                        // případ 3 - testovaná session zleva zasahuje do porovnávané uložené session
                        if ($startTested < $startSaved && $endTested > $startSaved && endTested <= $endSaved) {
                            //sessionsProcessing ($startTested, $startSaved, $type);  // rekurzivní test zbylého intervalu
                            addEventPairToArr ($startTested, $startSaved, $type);
                            return; 
                        }
                        // případ 4 - testovaná session zprava zasahuje do porovnávané uložené session
                        if ($startTested >= startSaved && $startTested < $endSaved && $endTested > $endSaved) {
                            //sessionsProcessing ($endSaved, $endTested, $type);      // rekurzivní test zbylého intervalu
                            addEventPairToArr ($endSaved, $endTested, $type);
                            return; 
                        }
                        // případ 5 - testovaná session oboustranně přesahuje porovnávanou uloženou session
                        if ($startTested < $startSaved && $endTested > $endSaved) {
                            //sessionsProcessing ($startTested, $startSaved, $type);  // rekurzivní test zbylého intervalu 1
                            //sessionsProcessing ($endSaved, $endTested, $type);      // rekurzivní test zbylého intervalu 2
                            addEventPairToArr ($startTested, $startSaved, $type);
                            addEventPairToArr ($endSaved, $endTested, $type);
                            return; 
                        }
                    }                     
                }
            }
        }
    }
    addEventPairToArr ($startTested, $endTested, $type);    // zbyde-li po rekurzích nějaký interval, uložím ho do pole $events
}
// ==============================================================================================================================================================================================

$users = $events = $queueGroup = [];                       // inicializace polí

// iterace queues -> sestavení pole párů fronta-skupina

foreach ($queues as $qNum => $q) {                          // iterace řádků tabulky front
    if ($qNum == 0) {continue;}                             // vynechání hlavičky tabulky
    $q_idqueue    = $q[0];
    $q_idinstance = $q[2];
    $q_idgroup    = $q[3];
    
    if ($q_idinstance != '3') {continue;}                   // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme
    if (array_key_exists($q_idqueue,$queueGroup)){continue;}// iterovaná fronta už je v poli párů fronta-skupina
    
    foreach ($groups as $gNum => $g) {                      // přiřazení skupiny k frontě
        if ($gNum == 0) {continue;}                         // vynechání hlavičky tabulky
        $g_idgroup = $g[0];
        if ($g_idgroup == $q_idgroup) {                     // fronta je přiřazena do nějaké skupiny
            $idgroup = $g_idgroup;
            break;
        }
        $idgroup = "";                                      // fronta není přiřazena do žádné skupiny
    }
    $queueGroup[$q_idqueue] = $idgroup;                     // zápis prvku do pole párů fronta-skupina
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// iterace queueSessions

foreach ($queueSessions as $qsNum => $qs) {             // foreach ($queueSessions as $qs)
    if ($qsNum == 0) {continue;}                        // vynechání hlavičky tabulky
    $idinstance = substr($qs[0], 0 ,1);                 // $qs[0] ... idqueuesession, 1. číslice určuje číslo instance (v tabulce není přímo idinstance)
    if ($idinstance != '3') {continue;}                 // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $startTime = $qs[1];
    $endTime   = !empty($qs[2]) ? $qs[2] : date('Y-m-d H:i:s');
    $idqueue   = $qs[4];
    $iduser    = $qs[5];

    $idgroup   = findInArray($idqueue, $queueGroup);
    $startDate = substr($startTime, 0, 10);
    $endDate   = substr($endTime,   0, 10);

    if ($startTime < $reportIntervTimes["start"] || $startTime > $reportIntervTimes["end"]) {continue;}
                                                        // session není ze zkoumaného časového rozsahu

    // session je ze zkoumaného časového rozsahu -> cyklus generující sessions pro všechny dny, po které trvala reálná session
    $processedDate = $startDate; 
    while ($processedDate <= $endDate) {                // parceluje delší než 1-denní sessions na části po dnech     
        $dayStartTime = max($startTime, $processedDate.' 00:00:00'); 
        $dayEndTime   = min($endTime  , $processedDate.' 23:59:59');            
        if ($dayStartTime < $dayEndTime) {              // eliminace nevalidních případů
            sessionsProcessing ($dayStartTime, $dayEndTime, "Q");
        }
        $processedDate = dateIncrem ($processedDate);   // inkrement data o 1 den
    }
}
// ==============================================================================================================================================================================================
// Get pause sessions + activities + records

// Get pause sessions   (pauseSessions nezávisí na skupinách, jen na uživatelích -> nelze je přiřazovat uživatelům jednotlivě, pouze sumárně v rámci prázdné skupiny)

foreach ($pauseSessions as $psNum => $ps) {         // foreach ($pauseSessions as $ps) {
    if ($psNum == 0) {continue;}                    // vynechání hlavičky tabulky
    $idinstance = substr($ps[0], 0 ,1);             // $ps[0] ... idpausesession, 1. číslice určuje číslo instance (v tabulce není přímo idinstance)
    if ($idinstance != '3') {continue;}             // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $startTime = $ps[1];
    $endTime   = !empty($ps[2]) ? $ps[2] : date('Y-m-d H:i:s');
    $iduser    = $ps[5];
    $idgroup   = "";                                // pauseSessions nejsou vázané na skupinu -> je použita prázdná skupina

    $startDate = substr($startTime, 0, 10);
    $endDate   = substr($endTime,   0, 10);

    if ($startTime < $reportIntervTimes["start"] || $startTime > $reportIntervTimes["end"]) {continue;}
                                                    // pauseSession není ze zkoumaného časového rozsahu

    // session je ze zkoumaného časového rozsahu -> cyklus generující sessions pro všechny dny, po které trvala reálná session
    $processedDate = $startDate; 
    while ($processedDate <= $endDate) {                // parceluje delší než 1-denní sessions na části po dnech     
        $dayStartTime = max($startTime, $processedDate.' 00:00:00'); 
        $dayEndTime   = min($endTime  , $processedDate.' 23:59:59');            
        if ($dayStartTime < $dayEndTime) {              // eliminace nevalidních případů
            sessionsProcessing ($dayStartTime, $dayEndTime, "P");
        }
        $processedDate = dateIncrem ($processedDate);   // inkrement data o 1 den
    }
}        
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// Get activities
/*
foreach ($activities as $aNum => $a) {
    if ($aNum == 0) {continue;}                     // vynechání hlavičky tabulky
    $idinstance = $a[18];
    if ($idinstance != '3') {continue;}             // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $idqueue   = $a[5];
    $iduser    = $a[6];
    $time      = $a[13];
    $typeAct   = $a[10];
    $timeOpen  = $a[15];
    $timeClose = !empty($a[16]) ? $a[16] : date('Y-m-d H:i:s');
    $itemJson  = $a[19];
    $item      = json_decode($itemJson, false);     // dekódováno z JSONu na objekt

    $idgroup   = findInArray($idqueue, $queueGroup);
    $date      = substr($time, 0, 10); 
    $dateOpen  = substr($timeOpen,  0, 10);
    $dateClose = substr($timeClose, 0, 10);

    if ($timeOpen < $reportIntervTimes["start"] || $timeClose > $reportIntervTimes["end"]) {continue;}
                                                    // aktivita není ze zkoumaného časového rozsahu nebo se netýká dané skupiny či uživatele

    // aktivita je ze zkoumaného časového rozsahu -> cyklus generující aktivity pro všechny dny, po které trvala reálná aktivita
    $processedDate = $dateOpen;

    while ($processedDate <= $dateClose) {          
        $dayStartTime = max($timeOpen,  $processedDate.' 00:00:00'); 
        $dayEndTime   = min($timeClose, $processedDate.' 23:59:59');
        if ($dayStartTime < $dayEndTime) {              // eliminace nevalidních případů
            sessionsProcessing ($dayStartTime, $dayEndTime, "A");
        }
        $processedDate = dateIncrem ($processedDate);   // inkrement data o 1 den        
    }
}     
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
//Get records

foreach ($records as $rNum => $r) {
    if ($rNum == 0) {continue;}                     // vynechání hlavičky tabulky
    $idinstance = $r[8];
    if ($idinstance != '3') {continue;}             // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $iduser   = $r[1];
    $idqueue  = $r[2];
    $edited   = $r[6];
    $idstatus = $r[3];
    $idcall   = $r[5];

    if (empty($iduser) || empty($edited) || empty($idstatus)) {continue;}   // vyřazení případných neúplných záznamů

    $idgroup    = findInArray($idqueue, $queueGroup);
    $editedDate = substr($edited, 0, 10);

    if ($editedDate < $reportIntervTimes["start"] || $editedDate > $reportIntervTimes["end"]) {continue;} 
                                                    // záznam není ze zkoumaného časového rozsahu nebo se netýká dané skupiny či uživatele

    // záznam je ze zkoumaného časového rozsahu
    initUsersAndEventsItems ($editedDate, $iduser, $idgroup);                
    if (!empty($idcall))         { $users[$editedDate][$iduser][$idgroup]["recordsTouched"] ++; }
    if ($idstatus == '00000021') { $users[$editedDate][$iduser][$idgroup]["recordsDropped"] ++; }   // Zavěsil zákazník
    if ($idstatus == '00000122') { $users[$editedDate][$iduser][$idgroup]["recordsTimeout"] ++; }   // Zavěsil systém
    if ($idstatus == '00000244') { $users[$editedDate][$iduser][$idgroup]["recordsBusy"]    ++; }   // Obsazeno
    if ($idstatus == '00000261') { $users[$editedDate][$iduser][$idgroup]["recordsDenied"]  ++; }   // Odmítnuto
}*/
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// sort pole uživatelů podle počtu hovorů v rámci dnů
/*
foreach ($users as $date => $daysByUserGroup) {
    foreach ($daysByUserGroup as $iduser => $daysByGroup) {
            foreach ($daysByGroup as $idgroup => $counters) {
            usort($counters, function($a, $b) {
                return $a["callCount"] < $b["callCount"];
            });
        }    
    }
}*/
// ==============================================================================================================================================================================================
//Do the events magic

foreach ($events as $date => $daysByUserGroup) {  

   foreach ($daysByUserGroup as $iduser => $daysByGroup) {
    
        foreach ($daysByGroup as $idgroup => $evnts) {          // sort pole událostí podle času v rámci dnů
            
            usort($evnts, function($a, $b) {
                return strcmp($a["time"], $b["time"]);
            });

            $times = [                                          // časy pro uspořádanou trojici [datum; skupina; uživatel]
                "Q"         => NULL,
                "QA"        => NULL,
                "QAP"       => NULL,
                "QP"        => NULL,
                "P"         => NULL,
                "AP"        => NULL
            ];
            $lastTime = 0;
            $status = [
                "Q" => false,
                "A" => false,
                "P" => false
            ];
            foreach ($evnts as $evnt) {
                $currentTime = strtotime($evnt["time"]);
                if ($lastTime > 0) {
                    switch ([$status["Q"], $status["A"], $status["P"]]) {                    
                        case [true , true , true ]:     $times["QAP"] += $currentTime - $lastTime;  break;
                        case [true , true , false]:     $times["QA" ] += $currentTime - $lastTime;  break;
                        case [true , false, false]:     $times["Q"  ] += $currentTime - $lastTime;  break;
                        case [true , false, true ]:     $times["QP" ] += $currentTime - $lastTime;  break;
                        case [false, false, true ]:     $times["P"  ] += $currentTime - $lastTime;  break;
                        case [false, true , true ]:     $times["AP" ] += $currentTime - $lastTime;
                        }
                    }
                switch ($evnt["method"]) {
                    case "+": $status[$evnt["type"]] = true;   break;
                    case "-": $status[$evnt["type"]] = false;
                }    
            $lastTime = $currentTime;
            }
            foreach ($times as $evnTyp => $evnTime) {
                $users[$date][$iduser][$idgroup][$evnTyp] = !is_null($evnTime) ? $evnTime : NULL;
            }
        }
    }
}        
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// Count idle time

foreach ($users as $date => $daysByUserGroup) {
    foreach ($daysByUserGroup as $iduser => $daysByGroup) {
        foreach ($daysByGroup as $idgroup => $counters) {
            $users[$date][$iduser][$idgroup]["idleTime"] = $counters["Q"];
        }
    }
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// Count transactions
/*
foreach ($users as $user) {
    //$userObject = TableRegistry::get('LogUser')->find()->where(['pbx_name' => $user->name, 'idpbxinstance' => 3])->first();
    if (!is_null($userObject)) {
        $user->transactionCount = TableRegistry::get('RewardTransaction')->find()->where(['idloguser' => $userObject->idloguser, 'time >=' => $date . ' 00:00:00', 'time <=' => $date . ' 23:59:59', 'type' => 'O'])->count();
        $sumary->transactionCount += $user->transactionCount;
    }
}
*/
// ==============================================================================================================================================================================================
// zápis záznamů do výstupních souborů       
        
// zápis denních časových úhrnů pro jednotlivé uživatele (produkční výstup)
foreach ($users as $date => $daysByUserGroup) {
    foreach ($daysByUserGroup as $iduser => $daysByGroup) {
        foreach ($daysByGroup as $idgroup => $counters) {
            $colVals = [$date, $idgroup];
            foreach ($counters as $attrVal) {
                $colVals[] = $attrVal;
            }
            $out_users -> writeRow($colVals);
        }
    }    
}

// zápis událostí (diagnostický výstup)
foreach ($events as $date => $daysByUserGroup) {
    foreach ($daysByUserGroup as $iduser => $daysByGroup) {
        foreach ($daysByGroup as $idgroup => $evnts) {        
            foreach ($evnts as $evnt) {
                $colVals = [$idgroup, $iduser];
                foreach ($evnt as $evntVal) { 
                    $colVals[] = $evntVal;
                }
                $out_events -> writeRow($colVals);
            }        
        }
    }    
}

?>