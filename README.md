# users_sessions
Предположим данные о событиях пользователей(сессиях) поступают каждый день в виде json( например, из Kafka). Нам необходимо ежедневно загружать этот json в ClickHouse и там его обрабатывать для удобного хранения всей истории пользовательских сессий.