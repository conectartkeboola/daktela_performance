<?php
// PERFORMANCE REPORT ODVOZENÝ Z OUT-BUCKETU DAKTELA

require_once "vendor/autoload.php";

// načtení konfiguračního souboru
$ds         = DIRECTORY_SEPARATOR;
$dataDir    = getenv("KBC_DATADIR");

// pro případ importu parametrů zadaných JSON kódem v definici PHP aplikace v KBC
$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// parametry importované z konfiguračního JSON v KBC
$reportIntervHistDays = $config['parameters']['reportIntervHistDays'];  // čas. rozsah historie pro tvorbu reportu - pole má klíče "start" a "end", kde musí být "start" >= "end"
$diagOutOptions       = $config['parameters']['diagOutOptions'];        // diag. výstup do logu Jobs v KBC - klíče: "basicStatusInfo", "queueGroupDump", "usersActivitiesDump", ...
                                                                        //                                 ... "eventsDump", "invalidRowsInfo", "invalidRowsDump", "eventsOutTable"
// ==============================================================================================================================================================================================

$tabsIn = ["groups", "queues", "queueSessions", "pauseSessions", "activities", "records"];    // vstupní tabulky

$tabsOut = [                                                                        // výstupní tabulky
    "users"     =>  ["date", "iduser", "idgroup", "Q", "QA", "QAP", "QP", "P", "AP", "queueSession", "pauseSession",
                     "talkTime", "idleTime", "activityTime", "callCount", "callCountAnswered", /*"transactionCount",*/
                     "recordsTouched", "recordsDropped", "recordsTimeout", "recordsBusy", "recordsDenied"],
    "events"    =>  ["iduser", "idgroup", "time", "type", "method"]
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

function dateIncrem ($datum, $days = 1) {                   // inkrement data o $days dní
    return date('Y-m-d',(strtotime($days.' day', strtotime($datum))));
}
function findInArray ($key, $arr) {
    return array_key_exists($key, $arr) ? $arr[$key] : "";
} 
function initUsersAndEventsItems ($date, $iduser, $idgroup) {
    global $users, $events;    
    // inicializace záznamů do pole uživatelů
    if (!array_key_exists($date,    $users))                 {$users[$date]                    = []; }
    if (!array_key_exists($iduser,  $users[$date]))          {$users[$date][$iduser]           = []; }
    if (!array_key_exists($idgroup, $users[$date][$iduser])) {$users[$date][$iduser][$idgroup] =
        [   "Q"                 => NULL,                    // sestavení záznamu do pole uživatelů
            "QA"                => NULL,
            "QAP"               => NULL,
            "QP"                => NULL,
            "P"                 => NULL,
            "AP"                => NULL,
            "queueSession"      => NULL,
            "pauseSession"      => NULL,
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
    }   
    // inicializace záznamů do pole událostí
    if (!array_key_exists($date,    $events))                 {$events[$date]                   = []; }
    if (!array_key_exists($iduser,  $events[$date]))          {$events[$date][$iduser]          = []; }
    if (!array_key_exists($idgroup, $events[$date][$iduser])) {$events[$date][$iduser][$idgroup]= []; }
}
function addEventPairToArr ($startTime, $endTime, $type) {              // zápis páru událostí (začátek - konec) do pole událostí
    global $processedDate, $iduser, $idgroup, $users, $events, $typeAct, $itemJson, $diagOutOptions;
    initUsersAndEventsItems ($processedDate, $iduser, $idgroup);
    switch ($type) {
        case "Q":   $users[$processedDate][$iduser][$idgroup]["queueSession"] += strtotime($endTime) - strtotime($startTime);    break;
        case "P":   $users[$processedDate][$iduser][$idgroup]["pauseSession"] += strtotime($endTime) - strtotime($startTime);    break;
        case "A":   if ($typeAct == 'CALL' && !empty($itemJson)) {
                        $item = json_decode($itemJson, false);          // dekódováno z JSONu na objekt
                        $users[$processedDate][$iduser][$idgroup]["activityTime"] += strtotime($endTime) - strtotime($startTime);
                        $users[$processedDate][$iduser][$idgroup]["talkTime"]     += $item-> duration;
                        $users[$processedDate][$iduser][$idgroup]["callCount"]    += 1;
                        if ($item-> answered == "true") {
                            $users[$processedDate][$iduser][$idgroup]["callCountAnswered"] += 1;
                        }
                    }
                    if ($diagOutOptions["usersActivitiesDump"]) {       // volitelný diagnostický výstup do logu
                        echo " \$users[".$processedDate."][".$iduser."][".$idgroup."] = ";
                        print_r($users[$processedDate][$iduser][$idgroup]);   
                        echo " || ";
                    }
    }
    $event1 = [ "time" => $startTime, "type" => $type, "method" => "+"];                                              
    $event2 = [ "time" => $endTime,   "type" => $type, "method" => "-"];    
    if ($diagOutOptions["eventsDump"]) {                                // volitelný diagnostický výstup do logu
        echo "\$startTime = ".$startTime." | \$endTime = ".$endTime." | \$type = ".$type." || ";
        print_r($event1); echo " || ";
        print_r($event2); echo " || ";
    }    
    $events[$processedDate][$iduser][$idgroup][] = $event1; 
    $events[$processedDate][$iduser][$idgroup][] = $event2;
}
function sessionTestedVsSaved($startTested, $endTested, $type, $evnts) {// čas začátku a konce testované session + typ testované session + ...
                                                                        // ... + pole událostí daného dne, uživatele a skupiny
    $startSaved = $endSaved = NULL;                                     // čas začátku a konce porovnávané uložené session
    foreach ($evnts as $evnt) {
        if ($evnt["type"]==$type && $evnt["method"]=="+") {$startSaved = $evnt["time"];}
        if ($evnt["type"]==$type && $evnt["method"]=="-" && !is_null($startSaved)) {$endSaved = $evnt["time"];}
        if (!is_null($startSaved) && !is_null($endSaved)) {                            
            // případ 1 - testovaná session leží celá v dřívějším nebo pozdějším čase než porovnávaná uložená session
            if (($startTested <  $startSaved && $endTested <= $startSaved) ||
                ($startTested >= $endSaved   && $endTested >  $endSaved) ) {
                $startSaved = $endSaved = NULL;             
                continue;       // testovaná session se s uloženou session nepřekrývá -> přechod k další uložené session
            }
            // případ 2 - testovaná session leží celá uvnitř porovnávané uložené session
            if ($startTested >= $startSaved && $startTested < $endSaved && $endTested <= $endSaved) {
                $startSaved = $endSaved = NULL; 
                return;        // testovaná session už je celá v poli $sessions -> return z funkce bez žádné akce
            }
            // případ 3 - testovaná session zleva zasahuje do porovnávané uložené session
            if ($startTested < $startSaved && $endTested > $startSaved && $endTested <= $endSaved) {
                sessionTestedVsSaved ($startTested, $startSaved, $type);  // rekurzivní test zbylého podintervalu
                $startSaved = $endSaved = NULL;
                return; 
            }
            // případ 4 - testovaná session zprava zasahuje do porovnávané uložené session
            if ($startTested >= $startSaved && $startTested < $endSaved && $endTested > $endSaved) {
                sessionTestedVsSaved ($endSaved, $endTested, $type);      // rekurzivní test zbylého podintervalu
                $startSaved = $endSaved = NULL; 
                return; 
            }
            // případ 5 - testovaná session oboustranně přesahuje porovnávanou uloženou session
            if ($startTested < $startSaved && $endTested > $endSaved) {
                sessionTestedVsSaved ($startTested, $startSaved, $type);  // rekurzivní test zbylého podintervalu 1
                sessionTestedVsSaved ($endSaved, $endTested, $type);      // rekurzivní test zbylého podintervalu 2
                $startSaved = $endSaved = NULL; 
                return; 
            } 
        }
    }
    addEventPairToArr($startTested, $endTested, $type); // nepřekrývá-li se testovaná session s žádnou uloženou session, uložím ji do pole $events
}
function sessionProcessing ($startTested, $endTested, $type) {
    global $processedDate, $iduser, $idgroup, $events;
    if (array_key_exists($processedDate, $events)) {
        if (array_key_exists($iduser, $events[$processedDate])) {
            if (array_key_exists($idgroup, $events[$processedDate][$iduser])) {                
                $evnts = $events[$processedDate][$iduser][$idgroup];    // pole událstí daného dne, uživatele a skupiny                 
                usort($evnts, function($a, $b) {                        // sort pole událostí daného dne, uživatele a skupiny podle času
                    return strcmp($a["time"], $b["time"]);
                });
                sessionTestedVsSaved ($startTested, $endTested, $type, $evnts);
            }
        }
    }
    addEventPairToArr($startTested, $endTested, $type);                 // testovaná session dosud není uložená -> uložím ji do pole $events
}
function sesionDayParcelation ($startTime, $endTime, $type) { 
    global $processedDate;                                  // proměnná se definuje uvnitř této fce, ale musí být přístupná v dalších fcích
    $startDate = substr($startTime, 0, 10);
    $endDate   = substr($endTime,   0, 10);        
    $processedDate = $startDate;
    while ($processedDate <= $endDate) {          
        $dayStartTime = max($startTime,           $processedDate .' 00:00:00'); 
        $dayEndTime   = min($endTime,  dateIncrem($processedDate).' 00:00:00');
        if ($dayStartTime < $dayEndTime) {                  // eliminace nevalidních případů
            sessionProcessing($dayStartTime, $dayEndTime, $type);
        }
        $processedDate = dateIncrem($processedDate);        // inkrement data o 1 den        
    }
}
// ==============================================================================================================================================================================================

$users = $events = $queueGroup = [];                        // inicializace polí

// iterace queues -> sestavení pole párů fronta-skupina
echo $diagOutOptions["basicStatusInfo"] ? "ZAHÁJENA ITERACE QUEUES & GROUPS... " : "";  // volitelný diagnostický výstup do logu

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
echo $diagOutOptions["basicStatusInfo"] ? "POLE \$queueGroup BYLO ÚSPĚŠĚ SESTAVENO... " : "";   // volitelný diagnostický výstup do logu
if ($diagOutOptions["queueGroupDump"]) {                    // volitelný diagnostický výstup do logu
    echo "\$queueGroup = ";
    print_r($queueGroup);
    echo " || ";
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// iterace queueSessions

echo $diagOutOptions["basicStatusInfo"] ? "ZAHÁJENA ITERACE QUEUESESSIONS... " : "";    // volitelný diagnostický výstup do logu

foreach ($queueSessions as $qsNum => $qs) {
    if ($qsNum == 0) {continue;}                        // vynechání hlavičky tabulky
    $idinstance = substr($qs[0], 0 ,1);                 // $qs[0] ... idqueuesession, 1. číslice určuje číslo instance (v tabulce není přímo idinstance)
    if ($idinstance != '3') {continue;}                 // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $startTime = $qs[1];
    $endTime   = !empty($qs[2]) ? $qs[2] : date('Y-m-d H:i:s');
    $idqueue   = $qs[4];
    $iduser    = $qs[5];
    $idgroup   = findInArray($idqueue, $queueGroup);

    if (empty($startTime) || empty($endTime) || empty($iduser)) {   // volitelný diagnostický výstup do logu + vyřazení případných neúplných záznamů
        if ($diagOutOptions["invalidRowsInfo"]) {echo "nevalidní záznam v QUEUESESSIONS... "; }
        if ($diagOutOptions["invalidRowsDump"]) {echo "\$qs = "; print_r($qs); }
        if ($diagOutOptions["invalidRowsInfo"] || $diagOutOptions["invalidRowsDump"]) {echo " || "; }
        continue;        
    }
    if ($startTime < $reportIntervTimes["start"] || $startTime > $reportIntervTimes["end"]) {continue;} // session není ze zkoumaného časového rozsahu

    sesionDayParcelation ($startTime, $endTime, "Q");   // session je ze zkoumaného čas. rozsahu -> cyklus generující sessions pro všechny dny, po které trvala reálná session
}
echo $diagOutOptions["basicStatusInfo"] ? "DOKONČENA ITERACE QUEUESESSIONS... ZAHÁJENA ITERACE PAUSESESSIONS... " : "";     // volitelný diagnostický výstup do logu
// ==============================================================================================================================================================================================
// iterace pauseSessions + activities + records
// iterace pauseSessions  (pauseSessions nezávisí na skupinách, jen na uživatelích -> nelze je přiřazovat uživatelům jednotlivě, pouze sumárně v rámci prázdné skupiny)

foreach ($pauseSessions as $psNum => $ps) {
    if ($psNum == 0) {continue;}                        // vynechání hlavičky tabulky
    $idinstance = substr($ps[0], 0 ,1);                 // $ps[0] ... idpausesession, 1. číslice určuje číslo instance (v tabulce není přímo idinstance)
    if ($idinstance != '3') {continue;}                 // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $startTime = $ps[1];
    $endTime   = !empty($ps[2]) ? $ps[2] : date('Y-m-d H:i:s');
    $iduser    = $ps[5];
    $idgroup   = "";                                    // pauseSessions nejsou vázané na skupinu -> je použita prázdná skupina
    
    if (empty($startTime) || empty($endTime) || empty($iduser)) {   // volitelný diagnostický výstup do logu + vyřazení případných neúplných záznamů
        if ($diagOutOptions["invalidRowsInfo"]) {echo "nevalidní záznam v PAUSESESSIONS | "; }
        if ($diagOutOptions["invalidRowsDump"]) {echo "\$ps = "; print_r($ps); }
        if ($diagOutOptions["invalidRowsInfo"] || $diagOutOptions["invalidRowsDump"]) {echo " || "; }
        continue;        
    }
    if ($startTime < $reportIntervTimes["start"] || $startTime > $reportIntervTimes["end"]) {continue;} // session není ze zkoumaného časového rozsahu 
    
    sesionDayParcelation ($startTime, $endTime, "P");   // session je ze zkoumaného čas. rozsahu -> cyklus generující sessions pro všechny dny, po které trvala reálná session
}
echo $diagOutOptions["basicStatusInfo"] ? "DOKONČENA ITERACE PAUSESESSIONS... ZAHÁJENA ITERACE AKTIVIT... " : "";   // volitelný diagnostický výstup do logu
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// iterace activities

foreach ($activities as $aNum => $a) {
    if ($aNum == 0) {continue;}                         // vynechání hlavičky tabulky
    $idinstance = $a[18];
    if ($idinstance != '3') {continue;}                 // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $idqueue   = $a[5];
    $iduser    = $a[6];
    $time      = $a[13];
    $typeAct   = $a[10];
    //$timeOpen  = $a[15];
    $timeClose = !empty($a[16]) ? $a[16] : date('Y-m-d H:i:s');
    $itemJson  = $a[19];
    $idgroup   = findInArray($idqueue, $queueGroup);
    //$dateOpen  = substr($timeOpen,  0, 10);
    
    if (empty($time) || empty($typeAct) || empty($iduser)) {    // volitelný diagnostický výstup do logu + vyřazení případných neúplných záznamů
        if ($diagOutOptions["invalidRowsInfo"]) {echo "nevalidní záznam v ACTIVITIES | "; }
        if ($diagOutOptions["invalidRowsDump"]) {echo "\$a = "; print_r($a); }
        if ($diagOutOptions["invalidRowsInfo"] || $diagOutOptions["invalidRowsDump"]) {echo " || "; }
        continue;        
    }
    if ($time < $reportIntervTimes["start"] || $time > $reportIntervTimes["end"]) {continue;} // aktivita není ze zkoumaného časového rozsahu

    // aktivita je ze zkoumaného časového rozsahu -> cyklus generující aktivity pro všechny dny, po které trvala reálná aktivita
    sesionDayParcelation ($time, $timeClose, "A");   
} 
echo $diagOutOptions["basicStatusInfo"] ? "DOKONČENA ITERACE AKTIVIT... ZAHÁJENA ITERACE RECORDS... " : "";     // volitelný diagnostický výstup do logu
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// iterace records

foreach ($records as $rNum => $r) {
    if ($rNum == 0) {continue;}                         // vynechání hlavičky tabulky
    $idinstance = $r[8];
    if ($idinstance != '3') {continue;}                 // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme

    $iduser   = $r[1];
    $idqueue  = $r[2];
    $edited   = $r[6];
    $idstatus = $r[3];
    $idcall   = $r[5];

    $idgroup    = findInArray($idqueue, $queueGroup);
    $editedDate = substr($edited, 0, 10);
    
    if (empty($edited) || empty($iduser)) {             // volitelný diagnostický výstup do logu + vyřazení případných neúplných záznamů
        if ($diagOutOptions["invalidRowsInfo"]) {echo "nevalidní záznam v RECORDS | "; }
        if ($diagOutOptions["invalidRowsDump"]) {echo "\$r = "; print_r($r); }
        if ($diagOutOptions["invalidRowsInfo"] || $diagOutOptions["invalidRowsDump"]) {echo " || "; }
        continue;        
    }
    if ($editedDate < $reportIntervTimes["start"] || $editedDate > $reportIntervTimes["end"]) {continue;}   // záznam není ze zkoumaného časového rozsahu

    // záznam je ze zkoumaného časového rozsahu
    initUsersAndEventsItems($editedDate, $iduser, $idgroup);
    if (!empty($idcall))         { $users[$editedDate][$iduser][$idgroup]["recordsTouched"] ++; }
    if ($idstatus == '00000021') { $users[$editedDate][$iduser][$idgroup]["recordsDropped"] ++; }   // zavěsil zákazník
    if ($idstatus == '00000122') { $users[$editedDate][$iduser][$idgroup]["recordsTimeout"] ++; }   // zavěsil systém
    if ($idstatus == '00000244') { $users[$editedDate][$iduser][$idgroup]["recordsBusy"]    ++; }   // obsazeno
    if ($idstatus == '00000261') { $users[$editedDate][$iduser][$idgroup]["recordsDenied"]  ++; }   // odmítnuto
}
echo $diagOutOptions["basicStatusInfo"] ? "DOKONČENA ITERACE RECORDS... " : "";                     // volitelný diagnostický výstup do logu
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
// events magic

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
// count idle time

foreach ($users as $date => $daysByUserGroup) {
    foreach ($daysByUserGroup as $iduser => $daysByGroup) {
        foreach ($daysByGroup as $idgroup => $counters) {
            $users[$date][$iduser][$idgroup]["idleTime"] = $counters["Q"];
        }
    }
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// count transactions
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
            if (!array_filter($counters)) {continue;}               // vyřazení případných záznamů obsahujících jen prázdné hodnoty
            $colVals = [$date, $iduser, $idgroup];
            foreach ($counters as $attrVal) {
                $colVals[] = $attrVal;
            }
            $out_users -> writeRow($colVals);
        }
    }    
}

// zápis událostí (volitelný diagnostický výstup)
if ($diagOutOptions["eventsOutTable"]) {
    foreach ($events as $date => $daysByUserGroup) {
        foreach ($daysByUserGroup as $iduser => $daysByGroup) {
            foreach ($daysByGroup as $idgroup => $evnts) {            
                foreach ($evnts as $evnt) {
                    if (!array_filter($evnt)) {continue;}               // vyřazení případných záznamů obsahujících jen prázdné hodnoty
                    $colVals = [$iduser, $idgroup];
                    foreach ($evnt as $evntVal) { 
                        $colVals[] = $evntVal;
                    }
                    $out_events -> writeRow($colVals);
                }        
            }
        }    
    }
}

?>