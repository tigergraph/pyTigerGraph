function tabhead(evt, cityName) {
    var i, tabcontent, tablinks;
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
      tabcontent[i].style.display = "none";
    }
    tablinks = document.getElementsByClassName("tablinks");
    for (i = 0; i < tablinks.length; i++) {
      tablinks[i].className = tablinks[i].className.replace(" active", "");
    }
    document.getElementById(cityName).style.display = "block";
    evt.currentTarget.className += " active";
  }
  
document.addEventListener('DOMContentLoaded', function(){
    var anchorHash = window.location.href.toString();
    if( anchorHash.lastIndexOf('#') != -1 ) {
        anchorHash = "tab_" + anchorHash.substr(anchorHash.lastIndexOf('#')+1);
        if( document.getElementById(anchorHash) != null ) {
           document.getElementById(anchorHash).click();
        } else {
          document.getElementById("tab_Enterprise").click();
        }
    } else {
        // Get the element with id="defaultOpen" and click on it
        document.getElementById("tab_Enterprise").click();
    }
});
