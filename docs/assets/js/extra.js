// Venice Documentation - Extra JavaScript

// Dynamic copyright year
document.addEventListener('DOMContentLoaded', function() {
  const yearElement = document.getElementById('current-year');
  if (yearElement) {
    yearElement.textContent = new Date().getFullYear();
  }
});
