
<?php
/**
 * This file is the entry of the web, which tail logs of Jenkins.
 * This file along with the PHPTail.php are downloaded from
 *   https://github.com/taktos/php-tail
 * This file is modified by @zixuan for jenkins use.
 */

/**
 * Require the library
 */
require_once 'PHPTail.php';

/**
 * Initilize a new instance of PHPTail
 * @var PHPTail
 */
$tail = new PHPTail($_GET['directory']);


/**
 * We're getting an AJAX call
 */
if(isset($_GET['ajax']))  {
    echo $tail->getNewLines($_GET['file'], $_GET['lastsize'], $_GET['grep'], $_GET['invert']);
    die();
}

/**
 * Regular GET/POST call, print out the GUI
 */
$tail->generateGUI();

