import DefaultTheme from 'vitepress/theme'
import './custom.css'
import Layout from './Layout.vue'
import CodeViewer from './CodeViewer.vue'

export default {
    extends: DefaultTheme,
    Layout,
    enhanceApp({ app }) {
        app.component('CodeViewer', CodeViewer)
    }
}