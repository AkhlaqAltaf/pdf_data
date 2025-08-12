
get_jazzmin_settings= {

    "order_with_respect_to": [
        "analytics_dashboard",
    ],
    "site_title": "VectorEye Admin Portal",
    "site_header": "VectorEye Admin Portal",
    "site_brand": "VectorEye",
    # "site_logo": "images/cyberrange/logo.svg",
    # "site_icon": "images/cyberrange/logo.svg",
    "set_language" :'/',
    "welcome_sign": "Welcome to the Admin Panel",
    "copyright": "VectorEye",


    "show_sidebar": True,
    "navigation_expanded": True,

    # Custom CSS/JS
    "custom_css": "jazzmin/cyberrange/css/custom-admin.css",
    "custom_js": "jazzmin/cyberrange/js/custom-admin.js",

    # Icons (use fontawesome class names)
    "icons": {
        # ACCOUNTS MODEL
        "accounts.CustomUser": "fas fa-user-shield",
        "auth": "fa fa-users-cog",
        "auth.user": "fa fa-user",
        "auth.group": "fa fa-users",
        "bid_record.bid_document": "fa fa-user-shield",
        "cont_record.contracts": "fa fa-comments",
        # CHALLENGES MODEL
        "challenges.Challenge": "fas fa-puzzle-piece",
        "challenges.DifficultyLevel": "fas fa-signal",
        "challenges.Category": "fas fa-tags",
        "challenges.ChallengeSource": "fas fa-link",
        "challenges.ChallengeType": "fas fa-shapes",
        "challenges.ChallengeQuestion": "fas fa-puzzle-piece",
        "challenges.ChallengeQuestionAnswer": "fas fa-check",
        "challenges.ChallengeFile": "fas fa-file-code",
        "challenges.ChallengeMachineSpawned": "fas fa-robot",
        "Challenges.Hint": "fas fa-flag",
        "Challenges.DocDockerImage": "fas fa-file-code",
        "Challenges.ChallengeQuestionAnswerOverride": "fas fa-edit",
        "Challenges.ChallengeQuestionAttemptOverride": "fas fa-check-circle",

        # ATTACKS MODEL
        "attacks.Attack": "fas fa-bolt",
        "attacks.AttackTimer": "fas fa-stopwatch",
        "attacks.AttackFlag": "fas fa-flag",
        "attacks.AttackLog": "fas fa-scroll",
        "attacks.AttackComment": "fas fa-comments",

        # HACKATHON MODELS
        "hackathon.HackathonCity": "fas fa-city",
        "hackathon.Hackathon": "fas fa-laptop-code",
        "hackathon.HackathonDateTimeDetail": "fas fa-calendar-alt",
        "hackathon.HackathonLogoDetails": "fas fa-image",
        "hackathon.PlayingHackathon": "fas fa-calendar-alt",


        # MEDIA MODEL
        "media.Media": "fas fa-photo-video",

        # TEAM MODELS
        "teams.ServiceInfo": "fas fa-server",
        "teams.Team": "fas fa-users",
        "teams.TeamMembership": "fas fa-id-badge",
        "teams.TeamPlayingHackathon": "fas fa-gamepad",
        "teams.Member": "fas fa-user-friends",
        "teams.TeamsScores": "fas fa-chart-line",
        "teams.TeamChallenges": "fas fa-brain",
        "teams.TeamRemoteMachine": "fas fa-network-wired",
        "teams.TeamRemoteMachineLogs": "fas fa-file-alt",

        # Notification MODELS
        "whisper.Notification": "fas fa-bell",

        "k8_services.KubernetesMachine": "fas  fa-link",

    },

    # "custom_links": {
    #     "accounts": [{
    #         "name": "Analysis",
    #         "url": "/admin/analysis/",
    #         "icon": "fas fa-chart-line",
    #     }],
    # },
    # Language options
    "language_chooser": False,
    "use_google_fonts_cdn": True,

}

get_jazzmin_ui_tweaks = {
    "theme": "flatly",
    "dark_mode_theme": "cyborg",
    "button_classes": {
        "primary": "btn-neon-primary",
        "secondary": "btn-outline-neon-secondary",
        "info": "btn-info",
        "warning": "btn-warning",
        "danger": "btn-danger",
        "success": "btn-success"
    },

    # UI Elements
    "navbar": "navbar-dark navbar-neon",
    "sidebar": "sidebar-dark-neon",
    "accent": "var(--neon-accent)",

    # Text Sizes
    "navbar_small_text": True,
    "footer_small_text": True,
    "body_small_text": False,
    "brand_small_text": False,
    "sidebar_nav_small_text": False,

    # Layout
    "sidebar_disable_expand": False,
    "sidebar_nav_child_indent": True,
    "sidebar_nav_compact_style": True,
    "sidebar_nav_legacy_style": False,
    "sidebar_nav_flat_style": True,

    "related_modal_active": True,
}

