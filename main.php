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
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
    // iterace queueSessions
    
    foreach ($queueSessions as $qsNum => $qs) {             // foreach ($queueSessions as $qs)
        if ($qsNum == 0) {continue;}                        // vynechání hlavičky tabulky
        $qs_idinstance = substr($qs[0], 0 ,1);              // $qs[0] ... idqueuesession, 1. číslice určuje číslo instance (v tabulce není přímo idinstance)
        if ($qs_idinstance != '3') {continue;}              // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme
        
        $qs_start_time = $qs[1];
        $qs_end_time   = !empty($qs[2]) ? $qs[2] : date('Y-m-d H:i:s');
        $qs_idqueue    = $qs[4];
        $qs_iduser     = $qs[5];
        
        $qs_idgroup    = findInArray($qs_idqueue, $queueGroup);
        $qs_start_date = substr($qs_start_time, 0, 10);
        $qs_end_date   = substr($qs_end_time,   0, 10);

        if (/*$qs_idqueue != $q_idqueue ||*/ $qs_start_time < $reportIntervTimes["start"] || $qs_start_time > $reportIntervTimes["end"]) {
            continue;                                       // queueSession není ze zkoumaného časového rozsahu nebo se netýká dané fronty
        }
        
        // queueSession je ze zkoumaného časového rozsahu -> cyklus generující queueSessions pro všechny dny, po které trvala reálná queueSession
        $processed_date = $qs_start_date;
        
        while ($processed_date <= $qs_end_date) {          
            
            $qsDay_start_time = max($qs_start_time, $processed_date.' 00:00:00'); 
            $qsDay_end_time   = min($qs_end_time  , $processed_date.' 23:59:59');
            
            if ($qsDay_end_time > $qsDay_start_time) {      // eliminace nevalidních případů
                initUsersAndEventsItems ($processed_date, $qs_iduser, $qs_idgroup);
                $users[$processed_date][$qs_iduser][$qs_idgroup]["queueSession"] += strtotime($qsDay_end_time) - strtotime($qsDay_start_time);
                $event1 = [ "time"      =>  $qsDay_start_time,
                            "type"      =>  "Q",
                            "method"    =>  "+"             
                ];
                $event2 = [ "time"      =>  $qsDay_end_time,
                            "type"      =>  "Q",
                            "method"    =>  "-"               
                ];
                $events[$processed_date][$qs_iduser][$qs_idgroup][] = $event1; 
                $events[$processed_date][$qs_iduser][$qs_idgroup][] = $event2;// zápis záznamů do pole událostí
            }
            $processed_date = dateIncrem($processed_date);  // inkrement data o 1 den
        }
    }
//}                                                           // konec iterace front  
// ==============================================================================================================================================================================================
// Get pause sessions + activities + records

//foreach ($users as $date => $daysByUserGroup) {

//    foreach ($daysByUserGroup as $iduser => $daysByGroup) {   

        // Get pause sessions   (pauseSessions nezávisí na skupinách, jen na uživatelích -> nelze je přiřazovat uživatelům jednotlivě, pouze sumárně v rámci prázdné skupiny)

        foreach ($pauseSessions as $psNum => $ps) {         // foreach ($pauseSessions as $ps) {
            if ($psNum == 0) {continue;}                    // vynechání hlavičky tabulky
            $ps_idinstance = substr($ps[0], 0 ,1);          // $ps[0] ... idpausesession, 1. číslice určuje číslo instance (v tabulce není přímo idinstance)
            if ($ps_idinstance != '3') {continue;}          // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme
            
            $ps_start_time = $ps[1];
            $ps_end_time   = !empty($ps[2]) ? $ps[2] : date('Y-m-d H:i:s');
            $ps_iduser     = $ps[5];

            $ps_start_date = substr($ps_start_time, 0, 10);
            $ps_end_date   = substr($ps_end_time,   0, 10);

            if (/*$ps_start_date != $date || $ps_iduser != $iduser*/ $ps_start_time < $reportIntervTimes["start"] || $ps_start_time > $reportIntervTimes["end"]) {continue;}
                                                            // pauseSession není ze zkoumaného časového rozsahu nebo se netýká daného uživatele

            // queueSession je ze zkoumaného časového rozsahu -> cyklus generující pauseSessions pro všechny dny, po které trvala reálná pauseSession
            $processed_date = $ps_start_date;

            while ($processed_date <= $ps_end_date) {          

                $psDay_start_time =  max($ps_start_time, $processed_date.' 00:00:00'); 
                $psDay_end_time   =  min($ps_end_time  , $processed_date.' 23:59:59');

                if ($psDay_end_time > $psDay_start_time) {  // eliminace nevalidních případů
                    initUsersAndEventsItems ($processed_date, $ps_iduser, "");
                    $users[$processed_date][$ps_iduser][""]["pauseSession"] += strtotime($psDay_end_time) - strtotime($psDay_start_time);
                                                            // [""] ... prázdná skupina - pro pauseSessions, které nezávisí na skupině
                    $event1 = [ "time"      =>  $psDay_start_time,
                                "type"      =>  "P",
                                "method"    =>  "+"
                    ];
                    $event2 = [ "time"      =>  $psDay_end_time,
                                "type"      =>  "P",
                                "method"    =>  "-"
                    ];
                    $events[$processed_date][$ps_iduser][""][] = $event1;
                    $events[$processed_date][$ps_iduser][""][] = $event2;                                  // [""] ... prázdná skupina
                }
                $processed_date = dateIncrem($processed_date);  // inkrement data o 1 den
            }
        }        
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                

        //foreach ($daysByGroup as $idgroup => $day) {
    /*    
            foreach ($queues as $qNum => $q) {                  // přiřazení fronty ke skupině
                if ($qNum == 0) {continue;}                     // vynechání hlavičky tabulky
                $q_idqueue = $q[0];
                $q_idgroup = $q[3];
                if ($q_idgroup == $idgroup) {
                    $idqueue = $q_idqueue;
                    break;
                }
            }
    */                                        
            // Get activities

            foreach ($activities as $aNum => $a) {
                if ($aNum == 0) {continue;}                     // vynechání hlavičky tabulky
                $a_idinstance = $a[18];
                if ($a_idinstance != '3') {continue;}           // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme
                
                $a_idqueue    = $a[5];
                $a_iduser     = $a[6];
                $a_time       = $a[13];
                $a_type       = $a[10];
                $a_time_open  = $a[15];
                $a_time_close = !empty($a[16]) ? $a[16] : date('Y-m-d H:i:s');
                $a_item       = $a[19];
                $item         = json_decode($a_item, false);    // dekódováno z JSONu na objekt

                $a_idgroup    = findInArray($a_idqueue, $queueGroup);
                $a_date       = substr($a_time, 0, 10);         
                $a_date_open  = substr($a_time_open,  0, 10);
                $a_date_close = substr($a_time_close, 0, 10);

                if (/*$a_date != $date  ||  $a_idqueue != $idqueue  ||  $a_iduser != $iduser*/ $a_time_open < $reportIntervTimes["start"] || $a_time_close > $reportIntervTimes["end"]) {continue;}
                                                                // aktivita není ze zkoumaného časového rozsahu nebo se netýká dané skupiny či uživatele

                // aktivita je ze zkoumaného časového rozsahu-> cyklus generující aktivity pro všechny dny, po které trvala reálná aktivita
                $processed_date = $a_date;

                while ($processed_date <= $a_date_close) {

                    $aDay_time_open  =  max($a_time_open,  $processed_date.' 00:00:00'); 
                    $aDay_time_close =  min($a_time_close, $processed_date.' 23:59:59');

                    if ($aDay_time_close > $aDay_time_open) {   // eliminace nevalidních případů
                        initUsersAndEventsItems ($processed_date, $a_iduser, $a_idgroup);
                        if ($a_type == 'CALL' && !empty($a_item)) {
                            $users[$processed_date][$a_iduser][$a_idgroup]["activityTime"] += strtotime($aDay_time_close) - strtotime($aDay_time_open);
                            $users[$processed_date][$a_iduser][$a_idgroup]["talkTime"]     += $item-> duration;      // parsuji duration z objektu $item
                            $users[$processed_date][$a_iduser][$a_idgroup]["callCount"]    += 1;
                            if ($item-> answered == "true") {   // parsuji answered z objektu $item
                                $users[$processed_date][$a_iduser][$a_idgroup]["callCountAnswered"] += 1;
                            }
                        }
                        $event1 = [ "time"      =>  $aDay_time_open,
                                    "type"      =>  "A",
                                    "method"    =>  "+"                   
                        ];
                        $event2 = [ "time"      =>  $aDay_time_close,
                                    "type"      =>  "A",
                                    "method"    =>  "-"                   
                        ];
                        $events[$processed_date][$a_iduser][$a_idgroup][] = $event1;
                        $events[$processed_date][$a_iduser][$a_idgroup][] = $event2;
                    }
                    $processed_date = dateIncrem($processed_date);  // inkrement data o 1 den
                }
            }     
            // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
            //Get records

            foreach ($records as $rNum => $r) {
                if ($rNum == 0) {continue;}                    // vynechání hlavičky tabulky
                $r_idinstance = $r[8];
                if ($r_idinstance != '3') {continue;}           // verze Daktely < 6  -> model neobsahuje tabulku 'activities' -> nezpracováváme
                
                $r_iduser     = $r[1];
                $r_idqueue    = $r[2];
                $r_edited     = $r[6];
                $r_idstatus   = $r[3];
                $r_idcall     = $r[5];
                
                $r_idgroup    = findInArray($r_idqueue, $queueGroup);
                $r_edited_date= substr($r_edited, 0, 10);

                if (/*$r_edited_date != $date  ||  $r_idqueue != $idqueue  ||  $r_iduser != $iduser*/ $r_edited_date < $reportIntervTimes["start"] || $r_edited_date > $reportIntervTimes["end"]) {continue;} 
                                                                // záznam není ze zkoumaného časového rozsahu nebo se netýká dané skupiny či uživatele

                // záznam je ze zkoumaného časového rozsahu
                initUsersAndEventsItems ($r_edited_date, $r_iduser, $r_idgroup);                
                if (!empty($r_idstatus) && !empty($r_idcall))         { $users[$r_edited_date][$r_iduser][$r_idgroup]["recordsTouched"] ++; }
                if (!empty($r_idstatus) && $r_idstatus == '00000021') { $users[$r_edited_date][$r_iduser][$r_idgroup]["recordsDropped"] ++; } // Zavěsil zákazník
                if (!empty($r_idstatus) && $r_idstatus == '00000122') { $users[$r_edited_date][$r_iduser][$r_idgroup]["recordsTimeout"] ++; } // Zavěsil systém
                if (!empty($r_idstatus) && $r_idstatus == '00000244') { $users[$r_edited_date][$r_iduser][$r_idgroup]["recordsBusy"]    ++; } // Obsazeno
                if (!empty($r_idstatus) && $r_idstatus == '00000261') { $users[$r_edited_date][$r_iduser][$r_idgroup]["recordsDenied"]  ++; } // Odmítnuto
            }            
//        } 
//    }    
//}                                                           // konec iterace users pro sessions + activities + records + TOTALS 
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
            
            usort($events[$date][$iduser], function($a, $b) {
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
            foreach ($events[$date][$iduser] as $evnt) {
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