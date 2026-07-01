# Docebo

[Docebo](https://www.docebo.com/) is a cloud-based learning management system (LMS) that helps organizations deliver, track, and manage their training programs.

Bruin supports Docebo as a source for [Ingestr assets](/assets/ingestr), enabling you to ingest data from your Docebo platform into your data warehouse.

## Connection

To configure a Docebo connection, you need:

- **Base URL**: Your Docebo instance URL (e.g., `https://yourcompany.docebosaas.com`)
- **Client ID**: OAuth2 client ID from your Docebo OAuth application
- **Client Secret**: OAuth2 client secret from your Docebo OAuth application
- **Username**: Your Docebo username
- **Password**: Your Docebo password

### Getting OAuth Credentials

1. Log in to your Docebo platform as a Super Admin
2. Navigate to **Settings** → **API and SSO**
3. Click on **Manage** to access the API and SSO settings
4. Go to the **API Credentials** tab
5. Create a new OAuth2 application:
   - Click **Add OAuth2 App**
   - Enter a name for your application
   - Configure the appropriate scopes for your data needs
   - Save the application
6. Note down the **Client ID** and **Client Secret**

## Configuration

Add the connection to `.bruin.yml`:

```yaml
connections:
  docebo:
    - name: "my-docebo"
      base_url: "https://yourcompany.docebosaas.com"
      client_id: "your_client_id"
      client_secret: "your_client_secret"  
      username: "your_username"
      password: "your_password"
```

## Ingestr Assets

Once you've configured the connection, you can create an Ingestr asset to ingest data from Docebo.

Here's an example asset configuration:

```yaml
name: docebo.users
type: ingestr

parameters:
  source_connection: my-docebo
  source_table: 'users'
  destination: bigquery
```

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `branches` | - | - | replace | Organizational units/branches in the org chart. Full reload on each run. |
| `categories` | - | - | replace | Course categories for organizing content. Full reload on each run. |
| `certifications` | - | - | replace | Certification programs and their configurations. Full reload on each run. |
| `course_enrollments` | - | - | replace | All course enrollment records with completion status. Full reload on each run. |
| `course_fields` | - | - | replace | Custom course field definitions. Full reload on each run. |
| `course_learning_objects` | - | - | replace | Learning objects (modules) within all courses. Full reload on each run. |
| `courses` | - | - | replace | All courses in the platform including e-learning, ILT, and webinars. Full reload on each run. |
| `external_training` | - | - | replace | External training records tracked in Docebo. Full reload on each run. |
| `group_members` | - | - | replace | Membership records for all groups. Full reload on each run. |
| `groups` | - | - | replace | User groups/audiences for organizing learners. Full reload on each run. |
| `learning_plan_course_enrollments` | - | - | replace | Course enrollments within learning plans. Full reload on each run. |
| `learning_plan_enrollments` | - | - | replace | User enrollments in learning plans. Full reload on each run. |
| `learning_plans` | - | - | replace | Learning plans (learning paths) that group courses. Full reload on each run. |
| `sessions` | - | - | replace | ILT/classroom sessions for instructor-led courses. Full reload on each run. |
| `user_fields` | - | - | replace | Custom user field definitions. Full reload on each run. |
| `users` | - | - | replace | All platform users including learners, instructors, and administrators. Full reload on each run. |

## Example: Ingesting User Data

Create a file `assets/docebo_users.asset.yml`:

```yaml
name: docebo.users
type: ingestr

parameters:
  source_connection: my-docebo
  source_table: 'users'
  destination: bigquery

columns:
  - name: user_id
    type: integer
    description: "Unique user identifier"
  - name: username
    type: string
    description: "User's username"
  - name: email
    type: string
    description: "User's email address"
  - name: first_name
    type: string
    description: "User's first name"
  - name: last_name
    type: string
    description: "User's last name"
  - name: created_at
    type: timestamp
    description: "User creation timestamp"
  - name: last_login
    type: timestamp
    description: "Last login timestamp"
```

## Notes

- Docebo integration currently supports **full refresh** only - incremental loading is not available
- The integration uses OAuth 2.0 password grant type for authentication
- Invalid date fields in the source data are normalized to Unix epoch (1970-01-01)
- Ensure your OAuth application has the necessary scopes to access the data you want to ingest

## Troubleshooting

### Authentication Issues

- Verify that your base URL is correct and includes the protocol (https://)
- Ensure your OAuth client has the correct permissions
- Check that your username and password are valid

### Data Access Issues

- Verify that your OAuth application has the necessary scopes
- Check that the user account has permissions to access the requested data
- Ensure the table name is spelled correctly (case-sensitive)

For more information on Ingestr assets, see the [Ingestr documentation](/assets/ingestr).
