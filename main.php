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

$users = $events = [];                                      // inicializace polí

foreach ($queues as $qNum => $q) {                          // iterace řádků tabulky front
    if ($qNum == 0) {continue;}                             // vynechání hlavičky tabulky
    $q_idqueue = $q[0];
    $q_idgroup = $q[3];
    
    foreach ($groups as $gNum => $g) {                      // přiřazení skupiny k frontě
        if ($gNum == 0) {continue;}                         // vynechání hlavičky tabulky
        $g_idgroup = $g[0];
        if ($g_idgroup == $q_idgroup) {
            $idgroup = $g_idgroup;
            break;
        }
    }
            
    foreach ($queueSessions as $qsNum => $qs) {             // foreach ($queueSessions as $qs)
        if ($qsNum == 0) {continue;}                        // vynechání hlavičky tabulky
        $qs_start_time = $qs[1];
        $qs_end_time   = !empty($qs[2]) ? $qs[2] : date('Y-m-d H:i:s');
        $qs_idqueue    = $qs[4];
        $qs_iduser     = $qs[5];
        
        $qs_start_date = substr($qs_start_time, 0, 10);
        $qs_end_date   = substr($qs_end_time,   0, 10);

        if ($qs_idqueue != $q_idqueue || $qs_start_time < $reportIntervTimes["start"] || $qs_start_time > $reportIntervTimes["end"]) {
            continue;                                       // queueSession není ze zkoumaného časového rozsahu nebo se netýká dané fronty
        }
        
        // queueSession je ze zkoumaného časového rozsahu -> cyklus generující queueSessions pro všechny dny, po které trvala reálná queueSession
        $processed_date = $qs_start_date;
        
        while ($processed_date <= $qs_end_date) {
            
            if (!in_array($processed_date, array_keys($users))) {
                $users[$processed_date] = [];
            }
            if (!in_array($qs_iduser, array_keys($users[$processed_date]))) {
                $users[$processed_date][$qs_iduser] = [];
                $users[$processed_date][""][$qs_iduser]= 0; // [""] ... prázdná skupina - pro pauseSessions, které nezávisí na skupině
            }
            if (!in_array($idgroup, array_keys($users[$processed_date][$qs_iduser]))) {
                $user = [                                   // sestavení záznamu do pole uživatelů
                    "iduser"            => $qs_iduser,
                    "Q"                 => 0,
                    "QA"                => 0,
                    "QAP"               => 0,
                    "QP"                => 0,
                    "P"                 => 0,
                    "AP"                => 0,
                    "queueSession"      => 0,
                    "pauseSession"      => "",              // pauseSessions nezávisí na skupinách -> počítají se v prázdné skupině
                    "talkTime"          => 0,
                    "idleTime"          => 0,                            
                    "activityTime"      => 0,
                    "callCount"         => 0,
                    "callCountAnswered" => 0,
                    //"transactionCount"  => 0,
                    "recordsTouched"    => 0,
                    "recordsDropped"    => 0,
                    "recordsTimeout"    => 0,
                    "recordsBusy"       => 0,
                    "recordsDenied"     => 0
                ];
                $users [$processed_date][$qs_iduser][$idgroup] = $user;     // zápis záznamu do pole uživatelů
                $users [$processed_date][$qs_iduser][""] = 0;               // pauseSessions nezávisí na skupinách -> počítají se v prázdné skupině
                $events[$processed_date][$qs_iduser][$idgroup] = [];        // inicializace záznamu do pole událostí
            }
            $qs_start_time =  max($qs_start_time, $processed_date.' 00:00:00'); 
            $qs_end_time   =  min($qs_end_time  , $processed_date.' 23:59:59');
            
            if ($qs_end_time > $qs_start_time) {            // eliminace nevalidních případů 
                $users[$processed_date][$qs_iduser][$idgroup]["queueSession"] += strtotime($qs_end_time) - strtotime($qs_start_time);
                $event1 = [
                    "time"      =>  $qs_start_time,
                    "type"      =>  "Q",
                    "method"    =>  "+"                   
                ];
                $event2 = [
                    "time"      =>  $qs_end_time,
                    "type"      =>  "Q",
                    "method"    =>  "-"                   
                ];
                $events[$processed_date][$qs_iduser][$idgroup][] = $event1; 
                $events[$processed_date][$qs_iduser][$idgroup][] = $event2;// zápis záznamů do pole událostí
            }
            $processed_date = date('Y-m-d',(strtotime( '+1 day', strtotime($processed_date))));     // inkrement data o 1 den
        }
    }
}                                                           // konec iterace front  
// ==============================================================================================================================================================================================
// Get pause sessions + activities + records

foreach ($users as $date => $daysByUserGroup) {

    foreach ($daysByUserGroup as $iduser => $daysByGroup) {   

        // Get pause sessions   (pauseSessions nezávisí na skupinách, jen na uživatelích -> nelze je přiřazovat uživatelům jednotlivě, pouze sumárně v rámci prázdné skupiny)

        foreach ($pauseSessions as $psNum => $ps) {         // foreach ($pauseSessions as $ps) {
            if ($psNum == 0) {continue;}                    // vynechání hlavičky tabulky
            $ps_start_time = $ps[1];
            $ps_end_time   = !empty($ps[2]) ? $ps[2] : date('Y-m-d H:i:s');
            $ps_iduser     = $ps[5];

            $ps_start_date = substr($ps_start_time, 0, 10);
            $ps_end_date   = substr($ps_end_time,   0, 10);

            if ($ps_start_date != $date  ||  $ps_iduser != $iduser) {continue;}
                                                            // pauseSession není ze zkoumaného časového rozsahu nebo se netýká daného uživatele

            // queueSession je ze zkoumaného časového rozsahu -> cyklus generující pauseSessions pro všechny dny, po které trvala reálná pauseSession
            $processed_date = $ps_start_date;

            while ($processed_date <= $ps_end_date) {

                $ps_start_time =  max($ps_start_time, $processed_date.' 00:00:00'); 
                $ps_end_time   =  min($ps_end_time  , $processed_date.' 23:59:59');

                if ($ps_end_time <= $ps_start_time) {       // eliminace nevalidních případů 
                    $users[$processed_date][""][$idgroup]["pauseSession"] += strtotime($ps_end_time) - strtotime($ps_start_time);
                                                            // [""] ... prázdná skupina - pro pauseSessions, které nezávisí na skupině
                    $event1 = [
                        "time"      =>  $ps_start_time,
                        "type"      =>  "P",
                        "method"    =>  "+"
                    ];
                    $event2 = [
                        "time"      =>  $ps_end_time,
                        "type"      =>  "P",
                        "method"    =>  "-"
                    ];
                    $events[$processed_date][""][$idgroup][] = $event1;
                    $events[$processed_date][""][$idgroup][] = $event2;                                 // [""] ... prázdná skupina
                }
                $processed_date = date('Y-m-d',(strtotime( '+1 day', strtotime($processed_date))));     // inkrement data o 1 den
            }
        }        
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                

        foreach ($daysByGroup as $idgroup => $day) {
            
            foreach ($queues as $qNum => $q) {                  // přiřazení fronty ke skupině
                if ($qNum == 0) {continue;}                     // vynechání hlavičky tabulky
                $q_idqueue = $q[0];
                $q_idgroup = $q[3];
                if ($q_idgroup != $idgroup) {
                    $idqueue = $q_idqueue;
                    break;
                }
            }
                                                
            // Get activities

            foreach ($activities as $aNum => $a) {
                if ($aNum == 0) {continue;}                     // vynechání hlavičky tabulky
                $a_idqueue    = $a[5];
                $a_iduser     = $a[6];
                $a_time       = $a[13];
                $a_type       = $a[10];
                $a_time_open  = $a[15];
                $a_time_close = !empty($a[16]) ? $a[16] : date('Y-m-d H:i:s');
                $a_item       = $a[19];
                $item         = json_decode($a_item, false);    // dekódováno z JSONu na objekt

                $a_date       = substr($a_time, 0, 10);            
                $a_date_open  = substr($a_time_open,  0, 10);
                $a_date_close = substr($a_time_close, 0, 10);

                if ($a_date != $date  ||  $a_idqueue != $idqueue  ||  $a_iduser != $iduser) {continue;}
                                                                // aktivita není ze zkoumaného časového rozsahu nebo se netýká dané skupiny či uživatele

                // aktivita je ze zkoumaného časového rozsahu-> cyklus generující aktivity pro všechny dny, po které trvala reálná aktivita
                $processed_date = $a_date;

                while ($processed_date <= $a_date_close) {

                    $a_time_open  =  max($a_time_open,  $processed_date.' 00:00:00'); 
                    $a_time_close =  min($a_time_close, $processed_date.' 23:59:59');

                    if ($a_time_close <= $a_time_open) {        // eliminace nevalidních případů 
                        if ($a_type == 'CALL' && !empty($item)) {
                            $users[$processed_date][$iduser][$idgroup]["activityTime"] += strtotime($a_time_close) - strtotime($a_time_open);
                            $users[$processed_date][$iduser][$idgroup]["talkTime"]     += $item-> duration;      // parsuji duration z objektu $item
                            $users[$processed_date][$iduser][$idgroup]["callCount"]    += 1;
                            if ($item-> answered == "true") {           // parsuji answered z objektu $item
                                $users[$processed_date][$iduser][$idgroup]["callCountAnswered"] += 1;
                            }
                        }
                        $event1 = [
                            "time"      =>  $a_time_open,
                            "type"      =>  "A",
                            "method"    =>  "+"                   
                        ];
                        $event2 = [
                            "time"      =>  $a_time_close,
                            "type"      =>  "A",
                            "method"    =>  "-"                   
                        ];
                        $events[$processed_date][$iduser][$idgroup][] = $event1;
                        $events[$processed_date][$iduser][$idgroup][] = $event2;
                    }
                    $processed_date = date('Y-m-d',(strtotime( '+1 day', strtotime($processed_date))));     // inkrement data o 1 den
                }
            }     
            // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
            //Get records

            foreach ($records as $rNum => $r) {
                if ($rNum == 0) {continue;}                     // vynechání hlavičky tabulky
                $r_iduser   = $r[1];
                $r_idqueue  = $r[2];
                $r_edited   = $r[6];
                $r_idstatus = $r[3];
                $r_idcall   = $r[5];            
                $r_edited_date = substr($r_edited, 0, 10);

                if ($r_edited_date != $date  ||  $r_idqueue != $idqueue  ||  $r_iduser != $iduser) {continue;} 
                                                                // záznam není ze zkoumaného časového rozsahu nebo se netýká dané skupiny či uživatele

                // záznam je ze zkoumaného časového rozsahu
                if (!empty($r_idstatus) && !empty($r_idcall))         { $users[$date][$iduser][$idgroup]["recordsTouched"] ++; }
                if (!empty($r_idstatus) && $r_idstatus == '00000021') { $users[$date][$iduser][$idgroup]["recordsDropped"] ++; } // Zavěsil zákazník
                if (!empty($r_idstatus) && $r_idstatus == '00000122') { $users[$date][$iduser][$idgroup]["recordsTimeout"] ++; } // Zavěsil systém
                if (!empty($r_idstatus) && $r_idstatus == '00000244') { $users[$date][$iduser][$idgroup]["recordsBusy"]    ++; } // Obsazeno
                if (!empty($r_idstatus) && $r_idstatus == '00000261') { $users[$date][$iduser][$idgroup]["recordsDenied"]  ++; } // Odmítnuto
            }            
        } 
    }    
}                                                           // konec iterace users pro sessions + activities + records + TOTALS 
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// sort pole uživatelů podle počtu hovorů v rámci dnů

foreach ($users as $date => $daysByUserGroup) {
    foreach ($daysByUserGroup as $iduser => $daysByGroup) {
            foreach ($daysByGroup as $idgroup => $counters) {
            usort($counters, function($a, $b) {
                return $a["callCount"] < $b["callCount"];
            });
        }    
    }
}
// ==============================================================================================================================================================================================
//Do the events magic

foreach ($events as $date => $daysByUserGroup) {  

   foreach ($daysByUserGroup as $iduser => $daysByGroup) {
    
        foreach ($daysByGroup as $idgroup => $evnts) {          // sort pole událostí podle času v rámci dnů
            
            usort($evnts, function($a, $b) {
                return strcmp($a["time"], $b["time"]);
            });

            $times = [                                          // časy pro uspořádanou trojici [datum; skupina; uživatel]
                "Q"         => 0,
                "QA"        => 0,
                "QAP"       => 0,
                "QP"        => 0,
                "P"         => 0,
                "AP"        => 0
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
                $users[$date][$iduser][$idgroup][$evnTyp] = $times[$evnTyp];
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
            foreach ($user as $attrVal) {
                $colVals[] = $attrVal;
            }
            $out_users -> writeRow($colVals);
        }
    }    
}

// zápis událostí (diagnostický výstup)
foreach ($users as $date => $daysByUserGroup) {
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