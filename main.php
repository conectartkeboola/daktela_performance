<?php
// PERFORMANCE REPORT ODVOZENÝ Z OUT-BUCKETU DAKTELA

require_once "vendor/autoload.php";

// načtení konfiguračního souboru
$ds         = DIRECTORY_SEPARATOR;
$dataDir    = getenv("KBC_DATADIR");

// pro případ importu parametrů zadaných JSON kódem v definici PHP aplikace v KBC
$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// ==============================================================================================================================================================================================

$tabsIn = ["queues", "queueSessions", "pauseSessions", "activities", "records"];     // vstupní tabulky

$tabsOut = [                                                        // výstupní tabulky
    "users"     => ["id", "queueSession", "pauseSession", "talkTime", "idleTime", "transactionCount", "activityTime", "callCount", "callCountAnswered"], 
    "userTimes" => ["iduser", "Q", "QA", "QAP", "QP", "P", "AP"],
    "data"      => ["activityTime", "queueSessionTime", "pauseSessionTime", "talkTime", "callCount", "callCountAnswered", "recordsTouched", "recordsDropped",
                    "recordsTimeout", "recordsBusy", "recordsDenied"]
];         
$tabsOutList = array_keys($tabsOut);
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

$date = is_null($date) ? date('Y-m-d',(strtotime( '-1 day', strtotime(date('Y-m-d'))))) /*date('Y-m-d')*/ : $date;
$events = [];

//foreach ($queues as $q) {
    //if ($q[0] == $queue) {
        //$this->set('currentQueue', $q);
foreach ($queues as $qNum => $q) {                                  // iterace řádků tabulky front
        if ($qNum == 0) {continue;}                                 // vynechání hlavičky tabulky
        $queue = $q[0];                                             // 0-idqueue
    
        if (!is_null($queue)) {
            $users = [];                                            // inicializace pole uživatelů
            // Get queue sessions and initiate users
            //$page = 0;
            //while (true) {
            //    $queueSessions = $pbxScript->getData(3, 'queueSessions', [['field' => 'start_time', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'start_time', 'value' => $date . ' 23:59:59', 'operator' => 'lte']], ['take' => 1000, 'skip' => $page * 1000]);
            foreach ($queueSessions as $qsNum => $qs) {             // foreach ($queueSessions as $qs) {
                if ($qsNum == 0) {continue;}                        // vynechání hlavičky tabulky
                if ($qs[1] < $date.' 00:00:00' || $qs[1] > $date.' 23:59:59') {     // záznam není z aktuálního dne
                    //unset ($queueSessions[$qsNum]);
                    continue;
                }
                if ($qs[4] == $queue) {                             // 4-idqueue    //if ($qs->queue->name == $queue) {                
                    if (!in_array($qs[5], array_keys($users))) {    // 5-iduser
                        $user = [                                   // sestavení záznamu do pole uživatelů
                            "id"                => $qs[5],          // 5-iduser
                            "queueSession"      => 0,
                            "pauseSession"      => 0,
                            "talkTime"          => 0,
                            "idleTime"          => 0,
                            "transactionCount"  => 0,
                            "activityTime"      => 0,
                            "callCount"         => 0,
                            "callCountAnswered" => 0
                        ];
                        $users [$qs[5]] = $user;   // $users[$qs->id_agent->name] = $user      // 5-iduser
                        $events[$qs[5]] = [];      // $events[$qs->id_agent->name] = [];       // 5-iduser
                    }
                    $user["queueSession"] += (!empty($qs[2]) ? strtotime($qs[2]) : time()) - strtotime($qs[1]); // 2-end_time; 1-start_time
                    $event1 = [
                        "time"      =>  $qs[1],                     // 1-start_time
                        "type"      =>  "Q",
                        "method"    =>  "+"                   
                    ];
                    $event2 = [
                        "time"      =>  !empty($qs[2]) ? $qs[2] : date('Y-m-d H:i:s'),  // 2-end_time
                        "type"      =>  "Q",
                        "method"    =>  "-"                   
                    ];
                    $events[$qs[5]][] = $event1;                    // 5-iduser
                    $events[$qs[5]][] = $event2;
                }
            }
        /*
                if (count($queueSessions) < 1000) {
                    break;        
                }
            $page++; */

        // Get pause sessions
        foreach ($users as $user) {
            //$pauseSessions = $pbxScript->getData(3, 'pauseSessions', [['field' => 'start_time', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'start_time', 'value' => $date . ' 23:59:59', 'operator' => 'lte'], ['field' => 'id_agent', 'value' => $user->name, 'operator' => 'eq']], ['take' => 1000]);
            foreach ($pauseSessions as $psNum => $ps) {             // foreach ($pauseSessions as $ps) {
                if ($psNum == 0) {continue;}                        // vynechání hlavičky tabulky
                if ($ps[1] < $date.' 00:00:00' || $ps[1] > $date.' 23:59:59') {     // záznam není z aktuálního dne
                    //unset ($pauseSessions[$psNum]);
                    continue;
                }
                $user["pauseSession"] += (!empty($ps[2]) ? strtotime($ps[2]) : time()) - strtotime($ps[1]);   // 2-end_time; 1-start_time
                $event1 = [
                    "time"      =>  $ps[1],                         // 1-start_time
                    "type"      =>  "P",
                    "method"    =>  "+"
                ];
                $event2 = [
                    "time"      =>  !empty($ps[2]) ? $ps[2] : date('Y-m-d H:i:s'),      // 2-end_time
                    "type"      =>  "P",
                    "method"    =>  "-"
                ];
                $events[$user["id"]][] = $event1;
                $events[$user["id"]][] = $event2;
            }
        }
        // Get activities
        foreach ($users as $user) {
            //$activities = $pbxScript->getData(3, 'activities', [['field' => 'time', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'time', 'value' => $date . ' 23:59:59', 'operator' => 'lte'], ['field' => 'user', 'value' => $user->name, 'operator' => 'eq']], ['take' => 1000, 'skip' => $page * 1000]);
            foreach ($activities as $aNum => $a) {                  // foreach ($activities as $a) {
                if ($psNum == 0) {continue;}                        // vynechání hlavičky tabulky
                if ($a[13] < $date.' 00:00:00' || $a[13] > $date.' 23:59:59') {     // záznam není z aktuálního dne;    13-time
                    //unset ($activities[$aNum]);
                    continue;
                }
                $item = json_decode($a[19], false);                 // 19-item, dekódováno z JSONu na objekt
                if ($a[10] == 'CALL' && !empty($item)) {            // 10-type 
                    $user["activityTime"] += (!empty($a[16]) ? strtotime($a[16]) : time()) - strtotime($a[15]); // 16-time_close; 15-time_open
                    $user["talkTime"]     += $item-> duration;      // parsuji duration z objektu $item
                    $user["callCount"]    += 1;
                    if ($item-> answered == "true") {               // parsuji answered z objektu $item
                        $user["callCountAnswered"] += 1;
                    }
                }
                $event1 = [
                    "time"      =>  $a[15],                         // 15-time_open
                    "type"      =>  "A",
                    "method"    =>  "+"                   
                ];
                $event2 = [
                    "time"      =>  !empty($a[16]) ? $a[16] : date('Y-m-d H:i:s'),      // 16-time_close
                    "type"      =>  "A",
                    "method"    =>  "-"                   
                ];
                $events[$user["id"]][] = $event1;
                $events[$user["id"]][] = $event2;
            }
        }
        //Get records
        $touched = $dropped = $timeout = $denied = $busy = 0;
        //while (true) {
            //$records = $pbxScript->getData(3, 'campaignsRecords', [['field' => 'edited', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'edited', 'value' => $date . ' 23:59:59', 'operator' => 'lte'], ['field' => 'queue', 'operator' => 'eq', 'value' => $queue]], ['take' => 1000, 'skip' => $page * 1000]);
        foreach ($records as $rNum => $r) {
            if ($rNum == 0) {continue;}                             // vynechání hlavičky tabulky
            if ($r[6] < $date.' 00:00:00' || $r[6] > $date.' 23:59:59') {     // záznam není z aktuálního dne;  6-edited
                    //unset ($records[$rNum]);
                    continue;
                }
            if (!empty($r[3]) && !empty($r[5]))       {$touched++;} // 3-idstatus; 5-idcall
            if (!empty($r[3]) && $r[3] == '00000021') {$dropped++;} // Zavěsil zákazník         //3-idstatus
            if (!empty($r[3]) && $r[3] == '00000122') {$timeout++;} // Zavěsil systém
            if (!empty($r[3]) && $r[3] == '00000244') {$busy++;   } // Obsazeno
            if (!empty($r[3]) && $r[3] == '00000261') {$denied++; } // Odmítnuto
        }
            //if (count($records) < 1000) {
            //   break;
            //}
            //$page++;
        //}
        //Count totals
        $data = [];
        $data["activityTime"] = $data["queueSessionTime"] = $data["pauseSessionTime"] = $data["talkTime"] = $data["callCount"] = $data["callCountAnswered"] = $data["idleTime"] = $data["transactionCount"] = 0;
        foreach ($users as $user) {
            $data["activityTime"]     += $user["activityTime"];
            $data["queueSessionTime"] += $user["queueSession"];
            $data["pauseSessionTime"] += $user["pauseSession"];
            $data["talkTime"]         += $user["talkTime"];
            $data["callCount"]        += $user["callCount"];
            $data["callCountAnswered"]+= $user["callCountAnswered"];
            $data["recordsTouched"]   =  $touched;
            $data["recordsDropped"]   =  $dropped;
            $data["recordsTimeout"]   =  $timeout;
            $data["recordsBusy"]      =  $busy;
            $data["recordsDenied"]    =  $denied;
        }
        usort($users, function($a, $b) {
            return $a["callCount"] < $b["callCount"];
        });
        //Do the events magic
        $userTimes = [];
        foreach ($events as $iduser => $evnts) {
            usort($evnts, function($a, $b) {
                return strcmp($a["time"], $b["time"]);
            });
            $times = [
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
                }
                switch ($evnt["method"]) {
                    case "+": $status[$event["type"]] = true;   break;
                    case "-": $status[$event["type"]] = false;
                }    
                $lastTime = $currentTime;
            $userTimes[$iduser] = $times;
        }        
        //}
        // Count idle time
        foreach ($userTimes as $iduser => $times) {
            $data["idleTime"] += $times["Q"];
            foreach ($users as $user) {
                if ($user["id"] == $iduser) {             // $user->name
                    $user["idleTime"] = $times["Q"];
                }
            }
        }
        /*
        // Count transactions
        foreach ($users as $user) {
            //$userObject = TableRegistry::get('LogUser')->find()->where(['pbx_name' => $user->name, 'idpbxinstance' => 3])->first();
            if (!is_null($userObject)) {
                $user->transactionCount = TableRegistry::get('RewardTransaction')->find()->where(['idloguser' => $userObject->idloguser, 'time >=' => $date . ' 00:00:00', 'time <=' => $date . ' 23:59:59', 'type' => 'O'])->count();
                $data->transactionCount += $user->transactionCount;
            }
        }

        // Set variables
        $this->set('users', $users);
        $this->set('userTimes', $userTimes);
        $this->set('data', $data);
        $this->set('date', $date);    */

        // ==============================================================================================================================================================================================

        // zápis záznamů do výstupních souborů       
        foreach ($users as $usr) {
           $out_users -> writeRow($usr);
        }    
        foreach ($userTimes as $iduser => $times) {
            $colVals = [
               $iduser,
               $times["Q"],
               $times["QA"],
               $times["QAP"],
               $times["QP"],
               $times["P"],
               $times["AP"]
            ];
            $out_userTimes -> writeRow($colVals);
        }    

        $out_data -> writeRow($data);   
    }
}
?>