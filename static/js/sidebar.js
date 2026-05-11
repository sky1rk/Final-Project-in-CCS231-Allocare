/**
 * Sidebar Toggle Functionality
 * Handles burger menu button and sidebar interactions on mobile/tablet
 */

document.addEventListener('DOMContentLoaded', function() {
    const sidebarToggle = document.getElementById('sidebar-toggle');
    const toggleLabel = document.querySelector('.sidebar-toggle-label');
    const sidebar = document.querySelector('.sidebar');
    const navLinks = document.querySelectorAll('.nav-link');
    const content = document.querySelector('.content');

    if (!sidebarToggle || !toggleLabel || !sidebar) {
        console.warn('Sidebar elements not found');
        return;
    }

    /**
     * Handle burger button click - toggle sidebar
     */
    toggleLabel.addEventListener('click', function(e) {
        e.preventDefault();
        sidebarToggle.checked = !sidebarToggle.checked;
    });

    /**
     * Close sidebar when clicking on a navigation link
     * This prevents the sidebar from staying open after navigation
     */
    navLinks.forEach(link => {
        link.addEventListener('click', function() {
            // Use setTimeout to allow link navigation before closing
            setTimeout(() => {
                sidebarToggle.checked = false;
            }, 50);
        });
    });

    /**
     * Close sidebar when clicking outside of it on mobile/tablet
     */
    if (content) {
        content.addEventListener('click', function(e) {
            // Only close if sidebar is open and viewport is small
            if (window.innerWidth <= 1024 && sidebarToggle.checked) {
                sidebarToggle.checked = false;
            }
        });
    }

    /**
     * Add overlay click handler - close sidebar when clicking overlay
     */
    document.addEventListener('click', function(e) {
        const isToggleButton = e.target === toggleLabel || toggleLabel.contains(e.target);
        const isSidebarContent = sidebar && (e.target === sidebar || sidebar.contains(e.target));
        const isNavLink = e.target.closest('.nav-link');
        
        // Close sidebar if clicking outside and sidebar is open
        if (window.innerWidth <= 1024 && sidebarToggle.checked && !isToggleButton && !isSidebarContent && !isNavLink) {
            sidebarToggle.checked = false;
        }
    });

    /**
     * Handle window resize - reset sidebar on desktop resize
     */
    window.addEventListener('resize', function() {
        if (window.innerWidth > 1024) {
            sidebarToggle.checked = false;
        }
    });

    /**
     * Keyboard support - close sidebar on Escape key
     */
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && sidebarToggle.checked && window.innerWidth <= 1024) {
            sidebarToggle.checked = false;
        }
    });
});
