// Use event listener to wait until DOM is loaded
document.addEventListener('DOMContentLoaded', function () {
  // Drops the useless "Group" tab from dbt docs
  const groupTab = document.querySelector('[ng-class="{active: (nav_selected == \'group\')}"]')
  if (groupTab) {
    groupTab.parentNode.remove()
  }

  // Open the "Database" tab and "awsdatacatalog" database by default
  const databaseTab = document.querySelector('[ng-class="{active: (nav_selected == \'database\')}"]')
  if (databaseTab) {
    databaseTab.click()
  }
  const targetTable = Array
    .from(document.querySelectorAll('span.filename-normal'))
    .find(span => span.textContent.trim() === 'awsdatacatalog')
  if (targetTable) {
    targetTable.click()
  }
})
