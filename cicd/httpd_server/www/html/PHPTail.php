<?php
/**
 * This file is the entry of the web, which tail logs of Jenkins.
 * This file along with the Log.php are downloaded from
 *   https://github.com/taktos/php-tail
 * This file is modified by @zixuan for jenkins use.
 */

class PHPTail {

    /**
     * Location of the log file we're tailing
     * @var string
     */
    private $log = "";
    /**
     * The time between AJAX requests to the server.
     *
     * Setting this value too high with an extremly fast-filling log will cause your PHP application to hang.
     * @var integer
     */
    private $updateTime;
    /**
     * This variable holds the maximum amount of bytes this application can load into memory (in bytes).
     * @var string
     */
    private $maxSizeToLoad;
    /**
     * @Zixuan
     * This store the log directory from URL parameter, and will be sent to recursive URL call
     */
    private $directory;
    /**
     *
     * PHPTail constructor
     * @param string $directory The log directory
     * @param integer $defaultUpdateTime The time between AJAX requests to the server.
     * @param integer $maxSizeToLoad This variable holds the maximum amount of bytes this application can load into memory (in bytes). Default is 2 Megabyte = 2097152 byte
     */
    public function __construct($directory, $defaultUpdateTime = 2000, $maxSizeToLoad = 2097152) {

        $this->directory = $directory;
        $files = scandir($directory);
        foreach ($files as &$value) {
          if ($value != '.' && $value != '..') {
            $this->log[$value] = $directory . '/' . $value;
          }
        }
        //print_r ($this->log);
        $this->updateTime = $defaultUpdateTime;
        $this->maxSizeToLoad = $maxSizeToLoad;
    }
    /**
     * This function is in charge of retrieving the latest lines from the log file
     * @param string $lastFetchedSize The size of the file when we lasted tailed it.
     * @param string $grepKeyword The grep keyword. This will only return rows that contain this word
     * @return Returns the JSON representation of the latest file size and appended lines.
     */
    public function getNewLines($file, $lastFetchedSize, $grepKeyword, $invert) {

        /**
         * Clear the stat cache to get the latest results
         */
        clearstatcache();
        /**
         * Define how much we should load from the log file
         * @var
         */
        if(empty($file) || is_dir($this->log[$file])) {
          return json_encode(array("size" => 0, "file" => "", "data" => []));
        }
        $fsize = filesize($this->log[$file]);
        $maxLength = ($fsize - $lastFetchedSize);
        /**
         * Verify that we don't load more data then allowed.
         */
        if($maxLength > $this->maxSizeToLoad) {
            $maxLength = ($this->maxSizeToLoad / 2);
        }
        /**
         * Actually load the data
         */
        $data = array();
        if($maxLength > 0) {

            $fp = fopen($this->log[$file], 'r');
            fseek($fp, -$maxLength , SEEK_END);
            $data = explode("\n", fread($fp, $maxLength));

        }
        /**
         * Run the grep function to return only the lines we're interested in.
         */
        if($invert == 0) {
            $data = preg_grep("/$grepKeyword/",$data);
        }
        else {
            $data = preg_grep("/$grepKeyword/",$data, PREG_GREP_INVERT);
        }
        /**
         * If the last entry in the array is an empty string lets remove it.
         */
        if(end($data) == "") {
            array_pop($data);
        }
        return json_encode(array("size" => $fsize, "file" => $this->log[$file], "data" => $data));
    }
    /**
     * This function will print out the required HTML/CSS/JS
     */
    public function generateGUI() {
        ?>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TigerGraph CI Service</title>

<link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css">
<link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css">
<link rel="stylesheet" href="//ajax.googleapis.com/ajax/libs/jqueryui/1.11.0/themes/smoothness/jquery-ui.css" />

<!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
<!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
<!--[if lt IE 9]>
    <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script>
    <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
<![endif]-->

<style type="text/css">
#grepKeyword, #settings {
    font-size: 80%;
}
.float {
    background: white;
    border-bottom: 1px solid black;
    padding: 10px 0 10px 0;
    margin: 0px;
    height: 30px;
    width: 100%;
    text-align: left;
}
.contents {
    margin-top: 30px;
}
.results {
    padding-bottom: 20px;
    font-family: monospace;
    font-size: small;
    white-space: pre;
}
.container {
    width: 1300px !important;
}
.dropdown-menu{
    height: auto;
    max-height: 500px;
    overflow-x: hidden;
}
.navbar-text {
  margin-left: 0 !important;
}
#grepspan {
  display: none;
}
#invertspan{
  display: none;
}
</style>

<script src="//ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
<script src="//ajax.googleapis.com/ajax/libs/jqueryui/1.11.0/jquery-ui.min.js"></script>
<script src="ansi_up.js" type="text/javascript"></script>

<script type="text/javascript">
    /* <![CDATA[ */
    //Last know size of the file
    lastSize = 0;
    //Grep keyword
    grep = "";
    //Should the Grep be inverted?
    invert = 0;
    //Last known document height
    documentHeight = 0;
    //Last known scroll position
    scrollPosition = 0;
    //Should we scroll to the bottom?
    scroll = true;
    lastFile = window.location.hash != "" ? window.location.hash.substr(1) : "";
    console.log(lastFile);
    $(document).ready(function() {

        // Setup the settings dialog
        $("#settings").dialog({
            modal : true,
            resizable : false,
            draggable : false,
            autoOpen : false,
            width : 590,
            height : 270,
            buttons : {
                Close : function() {
                    $(this).dialog("close");
                }
            },
            open : function(event, ui) {
                scrollToBottom();
            },
            close : function(event, ui) {
                grep = $("#grep").val();
                invert = $('#invert input:radio:checked').val();
                $("#results").text("");
                lastSize = 0;
                $("#grepspan").html("Grep keyword: \"" + grep + "\"");
                $("#invertspan").html("Inverted: " + (invert == 1 ? 'true' : 'false'));
            }
        });
        //Close the settings dialog after a user hits enter in the textarea
        $('#grep').keyup(function(e) {
            if (e.keyCode == 13) {
                $("#settings").dialog('close');
            }
        });
        //Focus on the textarea
        $("#grep").focus();
        //Settings button into a nice looking button with a theme
        //Settings button opens the settings dialog
        $("#grepKeyword").click(function() {
            $("#settings").dialog('open');
            $("#grepKeyword").removeClass('ui-state-focus');
        });
        $(".file").click(function(e) {
            $("#results").text("");
            lastSize = 0;
console.log(e);
            lastFile = $(e.target).text();
        });

        //Set up an interval for updating the log. Change updateTime in the PHPTail contstructor to change this
        setInterval("updateLog()", <?php echo $this->updateTime; ?>);
        //Some window scroll event to keep the menu at the top
        $(window).scroll(function(e) {
            if ($(window).scrollTop() > 0) {
                $('.float').css({
                    position : 'fixed',
                    top : '0',
                    left : 'auto'
                });
            } else {
                $('.float').css({
                    position : 'static'
                });
            }
        });
        //If window is resized should we scroll to the bottom?
        $(window).resize(function() {
            if (scroll) {
                scrollToBottom();
            }
        });
        //Handle if the window should be scrolled down or not
        $(window).scroll(function() {
            documentHeight = $(document).height();
            scrollPosition = $(window).height() + $(window).scrollTop();
            if (documentHeight <= scrollPosition + 50) {
                console.log('auto scroll')
                scroll = true;
            } else {
                console.log('no auto scroll')
                scroll = false;
            }
        });
        scrollToBottom();

    });
    //This function scrolls to the bottom
    function scrollToBottom() {
        $("html, body").animate({scrollTop: $(document).height()}, "fast");
    }
    //This function queries the server for updates.
    function updateLog() {
        $.getJSON('?directory=<?php echo $this->directory ?>&ajax=1&file=' + lastFile + '&lastsize=' + lastSize + '&grep=' + grep + '&invert=' + invert, function(data) {
            lastSize = data.size;
            $("#current").text(data.file);
            var ansi_up = new AnsiUp;
            $.each(data.data, function(key, value) {
                $("#results").append('   ' + ansi_up.ansi_to_html(value) + '<br/>');
            });
            if (scroll) {
                scrollToBottom();
            }
        });
    }
    /* ]]> */
</script>
</head>
<body>
    <div class="navbar navbar-default navbar-fixed-top" role="navigation">
        <div class="container">
            <div class="navbar-header">
                <a class="navbar-brand" href="#">CI Log System</a>
            </div>
            <div class="collapse navbar-collapse">
                <ul class="nav navbar-nav">
                    <li class="dropdown">
                        <a href="#" class="dropdown-toggle" data-toggle="dropdown">Files<span class="caret"></span></a>
                        <ul class="dropdown-menu" role="menu">
                            <?php foreach ($this->log as $title => $f): ?>
                              <?php if (is_dir($f)):  ?>
                                <li><a class="file dir" href="?directory=<?php echo $f;?>"><?php echo $title;?></a></li>
                              <?php else: ?>
                                <li><a class="file" href="#<?php echo $title;?>"><?php echo $title;?></a></li>
                              <?php endif; ?>
                            <?php endforeach;?>
                        </ul>
                    </li>
                    <li><a href="#" id="grepKeyword">Settings</a></li>
                    <li><span class="navbar-text" id="grepspan"></span></li>
                    <li><span class="navbar-text" id="invertspan"></span></li>
                </ul>
                <p class="navbar-text navbar-right" id="current"></p>
            </div>
        </div>
    </div>
    <div class="contents">
        <!--
          - In order to show the first line in the log, we set the 'margin-top'.
          - Because the GUI will print a bar and cover the first line content.
        -->
        <div style="margin-top: 60px;" id="results" class="results"></div>
        <div id="settings" title="Log settings">
            <p>Grep keyword (return results that contain this keyword)</p>
            <input id="grep" type="text" value="" />
            <p>Should the grep keyword be inverted? (Return results that do NOT contain the keyword)</p>
            <div id="invert">
                <input type="radio" value="1" id="invert1" name="invert" /><label for="invert1">Yes</label>
                <input type="radio" value="0" id="invert2" name="invert" checked="checked" /><label for="invert2">No</label>
            </div>
        </div>
    </div>
<script src="//maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
</body>
</html>
        <?php
    }
}
