// Use event listener to wait until DOM is loaded
document.addEventListener('DOMContentLoaded', function () {
  // Drops the useless "Group" tab from dbt docs
  const groupTab = document.querySelector('[ng-class="{active: (nav_selected == \'group\')}"]')
  if (groupTab) {
    groupTab.parentNode.remove()
  }
})
